"""
CLIP Service - Generate image embeddings using OpenAI CLIP model
"""
import os
import numpy as np
from typing import Union
from PIL import Image
import io
import torch
from transformers import CLIPProcessor, CLIPModel


class CLIPService:
    """Service to generate image embeddings using CLIP"""
    
    _instance = None
    
    def __init__(self):
        """Initialize CLIP model (singleton pattern for memory efficiency)"""
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"🔧 CLIP Loading on device: {self.device}")
        
        # Load pre-trained CLIP model
        self.model_name = "openai/clip-vit-base-patch32"
        self.model = CLIPModel.from_pretrained(self.model_name).to(self.device)
        self.processor = CLIPProcessor.from_pretrained(self.model_name)
        
        print(f"✅ CLIP Model loaded: {self.model_name}")
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = CLIPService()
        return cls._instance
    
    def get_image_embedding(self, image_data: Union[bytes, Image.Image]) -> np.ndarray:
        """
        Generate embedding for an image
        
        Args:
            image_data: Either bytes (file content) or PIL Image object
            
        Returns:
            np.ndarray: 512-dimensional embedding vector
        """
        try:
            # Convert bytes to PIL Image if needed
            if isinstance(image_data, bytes):
                image = Image.open(io.BytesIO(image_data))
            else:
                image = image_data
            
            # Convert RGBA to RGB if needed
            if image.mode == 'RGBA':
                image = image.convert('RGB')
            elif image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Preprocess image
            inputs = self.processor(
                images=image,
                return_tensors="pt"
            ).to(self.device)
            
            # Get image embedding
            with torch.no_grad():
                image_out = self.model.get_image_features(**inputs)
                # `get_image_features` may return a Tensor or a ModelOutput depending on
                # transformers version. Normalize appropriately.
                if isinstance(image_out, torch.Tensor):
                    image_features = image_out
                else:
                    # Try common attributes in ModelOutput
                    if hasattr(image_out, 'image_embeds'):
                        image_features = image_out.image_embeds
                    elif hasattr(image_out, 'pooler_output'):
                        image_features = image_out.pooler_output
                    elif hasattr(image_out, 'last_hidden_state'):
                        # fallback: mean pooling over sequence
                        image_features = image_out.last_hidden_state.mean(dim=1)
                    else:
                        raise RuntimeError(f"Unexpected CLIP model output: {type(image_out)}")

                # Normalize embedding (use functional normalize)
                image_embedding = torch.nn.functional.normalize(image_features, p=2, dim=-1)
            
            # Convert to numpy and flatten
            embedding = image_embedding.cpu().numpy().flatten().astype(np.float32)
            
            return embedding
        
        except Exception as e:
            raise ValueError(f"❌ Error generating image embedding: {str(e)}")
    
    def get_text_embedding(self, text: str) -> np.ndarray:
        """
        Generate embedding for text (bonus feature)
        
        Args:
            text: Text string to embed
            
        Returns:
            np.ndarray: 512-dimensional embedding vector
        """
        try:
            inputs = self.processor(
                text=text,
                return_tensors="pt"
            ).to(self.device)
            
            with torch.no_grad():
                text_features = self.model.get_text_features(**inputs)
                text_embedding = text_features / text_features.norm(dim=-1, keepdim=True)
            
            embedding = text_embedding.cpu().numpy().flatten().astype(np.float32)
            return embedding
        
        except Exception as e:
            raise ValueError(f"❌ Error generating text embedding: {str(e)}")


def get_clip_service() -> CLIPService:
    """Get or create CLIP service singleton"""
    return CLIPService.get_instance()
