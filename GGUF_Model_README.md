# Download and Use a GGUF Model for Local Llama Inference

## What is a GGUF Model?
A GGUF file is a format for quantized Llama and similar models, optimized for fast local inference using libraries like `llama-cpp-python`. You need a GGUF model file to run the local LLM in this RAG pipeline.

## How to Download a GGUF Model

### 1. Visit Hugging Face
- Go to: https://huggingface.co/TheBloke/Llama-2-7B-Chat-GGUF

### 2. Choose a Model File
- In the "Files and versions" section, select a file such as `llama-2-7b-chat.Q4_K_M.gguf` (Q4_K_M is a good balance of speed and quality).

### 3. Download the File
- If you have a Hugging Face account, click "Download".
- Or, use the following command in your terminal:
  ```bash
  wget https://huggingface.co/TheBloke/Llama-2-7B-Chat-GGUF/resolve/main/llama-2-7b-chat.Q4_K_M.gguf
  ```
  Or with curl:
  ```bash
  curl -O https://huggingface.co/TheBloke/Llama-2-7B-Chat-GGUF/resolve/main/llama-2-7b-chat.Q4_K_M.gguf
  ```

### 4. Place the File
- Move the downloaded `.gguf` file to your project directory, or specify its path in the Streamlit UI ("Llama GGUF Model Path").

## How to Use the GGUF Model in This Project
1. Ensure you have installed all dependencies:
   ```bash
   pip install -r rag/requirements.txt
   ```
2. Start the Streamlit app:
   ```bash
   streamlit run streamlit_app.py
   ```
3. In the sidebar, set the "Llama GGUF Model Path" to the location of your downloaded file (e.g., `./llama-2-7b-chat.Q4_K_M.gguf`).
4. Enter your query and run RAG as usual.

## Notes
- GGUF model files are large (several GB). Make sure you have enough disk space.
- You may need to accept the model license on Hugging Face before downloading.
- For best performance, use a quantized model (Q4 or Q5) suitable for your hardware.

## References
- [llama-cpp-python](https://github.com/abetlen/llama-cpp-python)
- [Hugging Face Model Page](https://huggingface.co/TheBloke/Llama-2-7B-Chat-GGUF)

---
For further help, see the main project README or contact the project maintainer.
