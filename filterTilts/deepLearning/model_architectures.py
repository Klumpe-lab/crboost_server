import torch.nn as nn
from torchvision.models.resnet import BasicBlock, ResNet

"""
To add models, define architectures as classes inheriting from nn.Module, and add them to the MODEL_REGISTRY.
"""

class SmallSimpleCNN(nn.Module):
    """CNN architecture for binary classification (good/bad tilts)."""

    # fc1 expects a 6x6 map after six 2x max-pools, so the network takes 384x384 only.
    input_size = 384

    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, kernel_size=3, stride=1, padding=1)
        self.bn1 = nn.BatchNorm2d(32)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1)
        self.bn2 = nn.BatchNorm2d(64)
        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, stride=1, padding=1)
        self.bn3 = nn.BatchNorm2d(128)
        self.conv4 = nn.Conv2d(128, 256, kernel_size=3, stride=1, padding=1)
        self.bn4 = nn.BatchNorm2d(256)
        self.conv5 = nn.Conv2d(256, 512, kernel_size=3, stride=1, padding=1)
        self.bn5 = nn.BatchNorm2d(512)
        self.conv6 = nn.Conv2d(512, 1024, kernel_size=3, stride=1, padding=1)
        self.bn6 = nn.BatchNorm2d(1024)
        
        self.fc1 = nn.Linear(1024 * 6 * 6, 1024)
        self.dropout1 = nn.Dropout(0.5)
        self.fc2 = nn.Linear(1024, 512)
        self.dropout2 = nn.Dropout(0.5)
        self.fc3 = nn.Linear(512, 256)
        self.dropout3 = nn.Dropout(0.5)
        self.fc4 = nn.Linear(256, 2)

        self.activationF = nn.ReLU()
        self.maxpool = nn.MaxPool2d(kernel_size=2, stride=2)
        
    def forward(self, x):
        x = self.activationF(self.bn1(self.conv1(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn2(self.conv2(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn3(self.conv3(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn4(self.conv4(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn5(self.conv5(x)))
        x = self.maxpool(x)
        x = self.activationF(self.bn6(self.conv6(x)))
        x = self.maxpool(x)
        x = x.view(x.size(0), -1)
        x = self.dropout1(self.activationF(self.fc1(x)))
        x = self.dropout2(self.activationF(self.fc2(x)))
        x = self.dropout3(self.activationF(self.fc3(x)))
        x = self.fc4(x)
        return x


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
    'SmallSimpleCNN': SmallSimpleCNN,
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