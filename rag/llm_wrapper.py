import os
from typing import List

#export OPENAI_API_KEY = "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"
try:
    import openai
except ImportError:
    openai = None

class LLMWrapper:
    def __init__(self, api_key=None, model="gpt-3.5-turbo"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        if openai:
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            self.client = None

    def generate(self, user_query: str, context: List[dict]) -> str:
        context_text = "\n".join([str(node) for node in context])
        prompt = f"Context:\n{context_text}\n\nUser Query: {user_query}\nAnswer:"
        if self.client:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant using knowledge graph context."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content.strip()
        else:
            return "[LLM not available: openai package not installed]"

# Example usage:
# llm = LLMWrapper()
# answer = llm.generate("Who won the match?", context)
