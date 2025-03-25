import os
import numpy as np

from andb.errno.errors import ConfigError
from andb.constants.ai_const import *


class ModelConfig(dict):
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            raise ConfigError(f'Not set configuration {key}')


class EmbeddingModelFactory:
    # singleton pattern
    __embedding_model = None
    __embedding_model_type = None

    @classmethod
    def create_model(cls, model_type, **config):
        """
        Factory method to initialize the correct EmbeddingModel subclass based on config.

        Args:
            model_type (str): The type of model to use
            config (dict): Configuration containing model type and related settings.

        Returns:
            EmbeddingModel: An instance of the appropriate subclass.
        """
        config = ModelConfig(config)
        # if user change model type, we need to reinit a new model
        if cls.__embedding_model and cls.__embedding_model_type == model_type:
            return cls.__embedding_model

        if model_type == "offline":
            cls.__embedding_model = OfflineEmbeddingModel(config)
        else:
            raise ValueError("Invalid model type. Choose 'offline'.")

        cls.__embedding_model_type = model_type
        return cls.__embedding_model


class EmbeddingModel:
    def generate_embeddings(self, text_list, normalize_embeddings=False):
        """
        Generate embeddings for the given text using the specified model.

        Args:
            text_list (list[str]): List of input text to encode into embeddings.
            normalize_embeddings: Flag to normalize the embeddings

        Returns:
            list[list[float]]: The generated embeddings as a list of floats.
        """
        raise NotImplementedError("generate_embeddings must be implemented in subclasses.")

class OfflineEmbeddingModel(EmbeddingModel):
    def __init__(self, config):
        from sentence_transformers import SentenceTransformer
        import torch

        self.model = SentenceTransformer(
            config.get("embed_offline_model_path"),
            device=config.get("device", "cuda" if torch.cuda.is_available() else "cpu")
        )

    def generate_embeddings(self, text_list, normalize_embeddings=False):
        embeddings_list = self.model.encode(text_list, normalize_embeddings=normalize_embeddings)
        return embeddings_list
