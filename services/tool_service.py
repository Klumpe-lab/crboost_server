# services/tool_service.py
from pathlib import Path
from typing import Dict, Any, Optional
from .config_service import get_config_service

class ToolService:
    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        
        # Define all available tools and their properties
        self.tools = {
            # Relion tools (container-based)
            'relion': {
                'type': 'container',
                'container': 'relion',
                'description': 'Relion tomography pipeline'
            },
            'relion_import': {
                'type': 'container', 
                'container': 'relion',
                'description': 'Relion tilt series import'
            },
            'relion_schemer': {
                'type': 'container',
                'container': 'relion', 
                'description': 'Relion pipeline scheduler'
            },
            
            # Warp tools (container-based)
            'warptools': {
                'type': 'container',
                'container': 'warp_aretomo',
                'description': 'WarpTools for motion correction and CTF estimation'
            },
            'aretomo': {
                'type': 'container',
                'container': 'warp_aretomo',
                'description': 'AreTomo for tilt series alignment'
            },
            
            # CryoCARE tools (container-based)
            'cryocare': {
                'type': 'container',
                'container': 'cryocare',
                'description': 'CryoCARE for cryo-EM denoising'
            },
            
            # PyTom tools (container-based)
            'pytom': {
                'type': 'container',
                'container': 'pytom',
                'description': 'PyTom for template matching'
            },
            
            # You could also add host-based tools in the future:
            # 'custom_script': {
            #     'type': 'host',
            #     'path': '/path/to/script',
            #     'description': 'Custom host-based script'
            # }
        }
    
    def get_tool(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get tool configuration by name"""
        return self.tools.get(tool_name)
    
    def is_container_tool(self, tool_name: str) -> bool:
        """Check if a tool is container-based"""
        tool = self.get_tool(tool_name)
        return tool and tool.get('type') == 'container'
    
    def get_container_for_tool(self, tool_name: str) -> Optional[str]:
        """Get container name for a container-based tool"""
        tool = self.get_tool(tool_name)
        return tool.get('container') if tool and tool.get('type') == 'container' else None

_tool_service = None

def get_tool_service() -> ToolService:
    global _tool_service
    if _tool_service is None:
        _tool_service = ToolService()
    return _tool_service