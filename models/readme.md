## ./models

This directory is purely for processing with sentiment models from HuggingFace. each "sentiment_process_*.py" automatically process each submissions/comments text data then add new field containing sentiment scores. 

### Model 1: 'gooohjy/suicidal-electra'
Model trained from Suicide and Depression Dataset obtained from Kaggle. For detailed info: https://huggingface.co/gooohjy/suicidal-electra
### Model 2: 'rafalposwiata/deproberta-large-depression'
(retrieved from model description)Fine-tuned DepRoBERTa model for detecting the level of depression as not depression, moderate or severe, based on social media posts in English. For detailed info: https://huggingface.co/rafalposwiata/deproberta-large-depression
### Model 3:'mrm8488/distilroberta-base-finetuned-suicide-depression'
distilroberta-base fine-tuned model of detect suicidal information. For detailed info: https://huggingface.co/mrm8488/distilroberta-base-finetuned-suicide-depression