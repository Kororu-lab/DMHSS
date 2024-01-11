from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

model_name = 'j-hartmann/emotion-english-distilroberta-base'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model.to(device)

def initialize_model():
    global model, tokenizer, device
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

def get_emotion_probabilities(text):
    if model is None or tokenizer is None:
        initialize_model()
    inputs = tokenizer(text, return_tensors='pt', truncation=True, max_length=512, padding=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    
    with torch.no_grad():
        outputs = model(**inputs)

    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    probs = probs.cpu().numpy()[0]
    return probs.tolist()  # [anger, disgust, fear, joy, neutral, sadness, surprise]


# If this file is run as a script, demonstrate the model initialization and a test run.
if __name__ == "__main__":
    initialize_model()
    test_text = "I've been feeling really down lately and don't know what to do."
    print(get_emotion_probabilities(test_text))
