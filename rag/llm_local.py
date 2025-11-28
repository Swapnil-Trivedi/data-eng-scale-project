import os
from typing import List
from llama_cpp import Llama

class LocalLLMWrapper:
    def __init__(self, model_path=None, n_ctx=2048, n_threads=4):
        self.model_path = model_path or os.getenv("LLAMA_MODEL_PATH", "./llama-2-7b-chat.Q4_K_M.gguf")
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"Model file not found: {self.model_path}\nDownload a GGUF model and set LLAMA_MODEL_PATH or pass model_path explicitly.")
        self.llm = Llama(model_path=self.model_path, n_ctx=n_ctx, n_threads=n_threads)

    def generate(self, user_query: str, context: List[dict], max_tokens=256) -> str:
        context_text = "\n".join([str(node) for node in context])
        prompt = f"Context:\n{context_text}\n\nUser Query: {user_query}\nAnswer:"
        output = self.llm(
            prompt,
            max_tokens=max_tokens,
            stop=["\nUser Query:", "\nContext:"]
        )
        return output["choices"][0]["text"].strip()

# Usage example:
# llm = LocalLLMWrapper(model_path="./llama-2-7b-chat.Q4_K_M.gguf")
# answer = llm.generate("Who won the match?", context)
