from andb.errno.errors import ConfigError
from andb.constants.ai_const import *

class ModelConfig(dict):
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            raise ConfigError(f'Not set configuration {key}')


class ClientModelFactory:
    # singleton pattern
    __client_model = None
    __client_model_type = None

    @classmethod
    def create_model(cls, model_type, **config):
        """
        Factory method to initialize the correct ClientModel subclass based on config.

        Args:
            model_type (str): The type of model to use
            config (dict): Configuration containing model type and related settings.

        Returns:
            ClientModel: An instance of the appropriate subclass.
        """
        config = ModelConfig(config)
        # if user change model type, we need to reinit a new model
        if cls.__client_model and cls.__client_model_type == model_type:
            return cls.__client_model

        if model_type == "external_api":
            cls.__client_model = ExternalAPIModel(config)
        else:
            raise ValueError("Invalid model type. Choose 'external_api'.")

        cls.__client_model_type = model_type
        return cls.__client_model

class ClientModel:
    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE):
        """
        Generate a completion for the given prompt using the specified model.

        Args:
            messages (list[dict]): A list of messages with roles and content.
            max_tokens (int): The maximum number of tokens to generate.
            temperature (float): Sampling temperature.
            stream (bool): Whether to stream the response (if supported).

        Returns:
            str: The generated completion.
        """
        raise NotImplementedError("complete_messages must be implemented in subclasses.")

class ExternalAPIModel(ClientModel):
    def __init__(self, config):
        import openai
        from openai import OpenAI

        self.external_api_model = config.get("client_external_api_model")
        openai.api_key = config.get("external_api_key")
        if self.external_api_model.startswith("deepseek"):
            self.openai_client = OpenAI(api_key=config.get("external_api_key"), base_url="https://api.deepseek.com")
        else:
            raise NotImplementedError("Other models are not implemented")

    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE):
        response = self.openai_client.chat.completions.create(
            model=self.external_api_model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        
        return response.choices[0].message.content
