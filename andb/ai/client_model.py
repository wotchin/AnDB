import os
import logging

from andb.constants.strings import OPENAI_API_KEY

DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_TOKENS = 1024

class ClientModelFactory:
    @staticmethod
    def create_model(config):
        """
        Factory method to initialize the correct ClientModel subclass based on config.

        Args:
            config (dict): Configuration containing model type and related settings.

        Returns:
            ClientModel: An instance of the appropriate subclass.
        """
        model_type = config.get("model_type")
        if model_type == "hf_api":
            return HFAPIModel(config)
        elif model_type == "openai":
            return OpenAIModel(config)
        elif model_type == "offline":
            return OfflineModel(config)
        else:
            raise ValueError("Invalid model type. Choose 'hf_api', 'openai', or 'offline'.")

class ClientModel:
    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE, stream=False):
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

class HFAPIModel(ClientModel):
    def __init__(self, config):
        from huggingface_hub import InferenceClient

        self.client = InferenceClient()
        self.model = config.get("hf_repo_id")

    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE, stream=False):
        response = self.client.chat_completion(
            messages,
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=stream,
        )
        
        if stream:
            return response
        else:
            return response.choices[0].message.content

class OpenAIModel(ClientModel):
    def __init__(self, config):
        import openai
        from openai import OpenAI

        openai.api_key = config["openai_api_key"]
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY') or config["openai_api_key"])
        self.openai_model = config["openai_model"]

    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE, stream=False):
        response = self.openai_client.chat.completions.create(
            model=self.openai_model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=stream,
        )
        
        if stream:
            return response
        else:
            return response.choices[0].message.content

class OfflineModel(ClientModel):
    def __init__(self, config):
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(config["model_path"])
        self.model = AutoModelForCausalLM.from_pretrained(
            config["model_path"],
            device_map="auto",
            torch_dtype="float16"
        )

    def complete_messages(self, messages, max_tokens=DEFAULT_MAX_TOKENS, temperature=DEFAULT_TEMPERATURE, stream=False):
        if stream:
            logging.warning("Streaming is not implemented for model loaded offline")
        
        # Use the tokenizer's chat template method
        input_ids = self.tokenizer.apply_chat_template(messages,
                                                        add_generation_prompt=True,
                                                        tokenize=True,
                                                        return_tensors="pt").to(self.model.device)

        # Generate response (should we add options for sampling?)
        outputs = self.model.generate(
            input_ids,
            max_new_tokens=max_tokens,
            eos_token_id=[self.tokenizer.eos_token_id] + self.tokenizer.additional_special_tokens_ids,
            temperature=temperature,
        )
        response = outputs[0][input_ids.shape[-1]:]
        return self.tokenizer.decode(response, skip_special_tokens=True)
    
