import torch.nn as nn
from torchvision.models.resnet import BasicBlock, ResNet

"""
To add models, define architectures as classes inheriting from nn.Module, and add them to the MODEL_REGISTRY.
"""

class ResNet18Gray(ResNet):
    """torchvision's ResNet-18 with a one-channel stem and a two-class head (bad = 0, good = 1),
    built the way the model author's trainTiltCNN_ResNet18 scripts build it, so their state dict
    loads unchanged (convert_checkpoint.py). Global average pooling takes any input size; 224 is
    the size the weights were trained at."""

    input_size = 224

    def __init__(self):
        super().__init__(BasicBlock, [2, 2, 2, 2])
        self.conv1 = nn.Conv2d(1, 64, kernel_size=7, stride=2, padding=3, bias=False)
        # The dropout rate does not matter at inference; the Linear sits at index 1 as in the source.
        self.fc = nn.Sequential(nn.Dropout(), nn.Linear(512, 2))


# Model Registry - maps model names to classes
MODEL_REGISTRY = {
    'ResNet18Gray': ResNet18Gray,
}


def get_model_class(model_name):
    """
    Get model class by name.
    
    Parameters:
    - model_name: Name of the model architecture
    
    Returns:
    - Model class
    """
    if model_name not in MODEL_REGISTRY:
        available = ', '.join(MODEL_REGISTRY.keys())
        raise ValueError(f"Unknown model: {model_name}. Available models: {available}")
    
    return MODEL_REGISTRY[model_name]


def get_available_models():
    """
    Get list of available model names.
    
    Returns:
    - List of model names
    """
    return list(MODEL_REGISTRY.keys())