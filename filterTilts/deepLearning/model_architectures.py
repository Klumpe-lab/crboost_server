import torch.nn as nn

"""
To add models, define architectures as classes inheriting from nn.Module, and add them to the MODEL_REGISTRY.
"""

class SmallSimpleCNN(nn.Module):
    """CNN architecture for binary classification (good/bad tilts)."""
    
    def __init__(self):
        super(SmallSimpleCNN, self).__init__()
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


# Model Registry - maps model names to classes
MODEL_REGISTRY = {
    'SmallSimpleCNN': SmallSimpleCNN,
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