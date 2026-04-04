import torch
from pathlib import Path
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms
from .model_architectures import get_model_class, get_available_models, MODEL_REGISTRY


class PILImageDataset(Dataset):
    """Custom dataset for PIL images.
    Required to create a DataLoader for batch processing and GPU inference."""
    
    def __init__(self, pil_images, transform=None):
        self.pil_images = pil_images
        self.transform = transform
        
    def __len__(self):
        return len(self.pil_images)
    
    def __getitem__(self, idx):
        img = self.pil_images[idx]
        
        # Convert PIL to grayscale if needed
        if img.mode != 'L':
            img = img.convert('L')
        
        # Apply transforms
        if self.transform:
            img = self.transform(img)
        
        return img


class ModelLoader:
    """Handles loading and running inference (prediction) with PyTorch models."""
    
    def __init__(self, model_path, gpu=0, model_architecture=None, num_workers=4):
        """
        Initialize model loader.
        
        Parameters:
        - model_path: Path to .pth model file
        - gpu: GPU device number (0 for first GPU, -1 for CPU)
        - model_architecture: Name of model architecture. If None, will try to auto-detect from checkpoint
        - num_workers: Number of workers for DataLoader
        """
        if model_path == 'default':
            self.model_path = Path(__file__).parent / 'data' / 'models' / 'michaelNet_0.6' / 'model.pth'
        else:
            self.model_path = Path(model_path)
        self.gpu = gpu
        self.num_workers = num_workers 
        self.model_architecture = model_architecture
        self.model = None
        self.device = None
        self.vocab = ['bad', 'good']  # Class labels (index 0=bad, 1=good)
        
        # Define the same transforms used during validation
        self.transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.5], std=[0.5])
        ])
        
        # Setup device (GPU or CPU)
        self._setup_device()
        
    def _setup_device(self):
        """Setup GPU or CPU device."""
        if self.gpu >= 0 and torch.cuda.is_available():
            self.device = torch.device(f'cuda:{self.gpu}')
            torch.cuda.set_device(self.gpu)
            print(f"Using GPU {self.gpu}")
        else:
            self.device = torch.device('cpu')
            print("Using CPU")
    
    def load_model(self):
        """Load PyTorch model from .pth file. Checkpoint dict should contain 'model_architecture' key with same
        name as in the model_architectures.py file."""

        '''
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model not found: {self.model_path}")
        
        print(f"Loading model from: {self.model_path}")
        
        # Load checkpoint
        checkpoint = torch.load(self.model_path, map_location='cpu')
        
        # Get architecture used by the model from the ckpt dict
        if self.model_architecture is None:
            if 'model_architecture' in checkpoint:
                self.model_architecture = checkpoint['model_architecture']
                print(f"Using architecture from checkpoint: {self.model_architecture}")
            elif self.model_path.stem in MODEL_REGISTRY:
                self.model_architecture = MODEL_REGISTRY[self.model_path.stem]
                print(f"Using architecture from filename: {self.model_architecture}")
            else:
                # If the name of the model architecture is not found, or just weights are saved, use default architecture
                self.model_architecture = 'SmallSimpleCNN'
                print(f"WARNING: No architecture found in checkpoint dict, using default: {self.model_architecture}")
        else:
            print(f"Using specified architecture: {self.model_architecture}")
        
        # Get model class and instantiate
        ModelClass = get_model_class(self.model_architecture)
        self.model = ModelClass()
        
        # Load weights
        # if model weights are saved as part of a checkpoint dict (better)
        if 'model_state_dict' in checkpoint:
            self.model.load_state_dict(checkpoint['model_state_dict'])
        # or if only the state_dict (weights) is saved
        else:
            self.model.load_state_dict(checkpoint)
        
        self.model.to(self.device)
        self.model.eval()
        
        print(f"Model loaded successfully: {self.model_architecture}")
        return self.model
        '''
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model not found: {self.model_path}")
        
        print(f"Loading model from: {self.model_path}")
        
        # Clear GPU cache before loading
        if self.device.type == 'cuda':
            torch.cuda.empty_cache()
            allocated = torch.cuda.memory_allocated(self.gpu) / 1024**3
            reserved = torch.cuda.memory_reserved(self.gpu) / 1024**3
            total = torch.cuda.get_device_properties(self.gpu).total_memory / 1024**3
            print(f"GPU {self.gpu} memory: {allocated:.2f} GB allocated, {reserved:.2f} GB reserved, {total:.2f} GB total")
        
        # Load checkpoint to CPU
        checkpoint = torch.load(self.model_path, map_location='cpu')
        
        # Get architecture
        if self.model_architecture is None:
            if 'model_architecture' in checkpoint:
                self.model_architecture = checkpoint['model_architecture']
                print(f"Using architecture from checkpoint: {self.model_architecture}")
            elif self.model_path.stem in MODEL_REGISTRY:
                self.model_architecture = MODEL_REGISTRY[self.model_path.stem]
                print(f"Using architecture from filename: {self.model_architecture}")
            else:
                self.model_architecture = 'SmallSimpleCNN'
                print(f"WARNING: No architecture found, using default: {self.model_architecture}")
        else:
            print(f"Using specified architecture: {self.model_architecture}")
        
        # Create model
        ModelClass = get_model_class(self.model_architecture)
        self.model = ModelClass()
        
        # Load weights
        if 'model_state_dict' in checkpoint:
            self.model.load_state_dict(checkpoint['model_state_dict'])
        else:
            self.model.load_state_dict(checkpoint)
        
        print("Weights loaded")
        
        # Free checkpoint memory
        del checkpoint
        
        # Try to move to GPU, fallback to CPU if OOM
        try:
            self.model.to(self.device)
            print(f"✓ Model moved to {self.device}")
            
            if self.device.type == 'cuda':
                mem_used = torch.cuda.memory_allocated(self.gpu) / 1024**3
                print(f"GPU {self.gpu} memory after loading: {mem_used:.2f} GB")
        
        except RuntimeError as e:
            if "out of memory" in str(e):
                print(f"\n⚠ GPU OUT OF MEMORY!")
                print(f"Original device: {self.device}")
                print("Falling back to CPU...")
                
                # Clear GPU and switch to CPU
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                
                self.device = torch.device('cpu')
                self.model.to(self.device)
                print(f"✓ Model moved to CPU")
            else:
                raise e
        
        self.model.eval()
        print(f"Model ready for inference on {self.device}")
        
        return self.model


    def create_test_dataloader(self, pil_images, batch_size=50):
        """
        Create test dataloader from PIL images.
        
        Parameters:
        - pil_images: List of PIL Image objects
        - batch_size: Batch size for inference
        
        Returns:
        - DataLoader object
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load_model() first.")
        
        # Create dataset with transforms
        dataset = PILImageDataset(pil_images, transform=self.transform)
        
        # Create dataloader
        test_dl = DataLoader(
            dataset, 
            batch_size=batch_size, 
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=(self.device.type == 'cuda')
        )
        
        return test_dl
    
    def predict_batch(self, pil_images, batch_size=50):
        """
        Run predictions on batch of PIL images.
        
        Parameters:
        - pil_images: List of PIL Image objects
        - batch_size: Batch size for inference
        
        Returns:
        - Tuple of (predictions, probabilities)
        """
        if self.model is None:
            self.load_model()
        
        # Create dataloader
        test_dl = self.create_test_dataloader(pil_images, batch_size)
        
        # Run predictions
        all_outputs = []
        
        self.model.eval()
        with torch.no_grad():
            for inputs in test_dl:
                inputs = inputs.to(self.device)
                outputs = self.model(inputs)
                all_outputs.append(outputs.cpu())
        
        # Concatenate all predictions
        preds = torch.cat(all_outputs, dim=0)
        
        # Apply softmax to get probabilities
        pred_probs_tensor = torch.softmax(preds, dim=1)
        
        # Get predicted labels (argmax)
        pred_indices = torch.argmax(pred_probs_tensor, dim=1).numpy()
        
        # Get maximum probability for each prediction
        pred_probs_max = pred_probs_tensor.max(dim=1).values.numpy()
        
        # Convert indices to labels
        pred_labels = [self.vocab[idx] for idx in pred_indices]
        
        # Convert to list
        pred_probs = pred_probs_max.tolist()
        
        return pred_labels, pred_probs
    
    @staticmethod
    def get_available_architectures():
        """Get list of available model architectures."""
        return get_available_models()