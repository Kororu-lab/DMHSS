from transformers import RobertaTokenizer, RobertaForSequenceClassification
import torch

model_name = 'rafalposwiata/deproberta-large-depression'
model = None
tokenizer = None
device = None

def initialize_model():
    global model, tokenizer, device
    model = RobertaForSequenceClassification.from_pretrained(model_name)
    tokenizer = RobertaTokenizer.from_pretrained(model_name)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

def get_label_probabilities(text):
    if model is None or tokenizer is None:
        initialize_model()
    
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
    inputs = {name: tensor.to(device) for name, tensor in inputs.items()}
    
    with torch.no_grad():
        outputs = model(**inputs)
    
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    probs = probs.cpu().numpy()[0]
    
    # Returning the probability of label 1 (suicide)
    return [probs[0], probs[1], probs[2]]  # [severe, moderate, not severe]
