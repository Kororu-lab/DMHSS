from transformers import ElectraTokenizer, ElectraForSequenceClassification
import torch

model_name = 'gooohjy/suicidal-electra'
model = None
tokenizer = None
device = None

def initialize_model():
    global model, tokenizer, device
    model = ElectraForSequenceClassification.from_pretrained(model_name)
    tokenizer = ElectraTokenizer.from_pretrained(model_name)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

def get_label_probabilities(text):
    if model is None or tokenizer is None:
        initialize_model()
    
    # Tokenize the text
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)

    # Move inputs to device
    inputs = {name: tensor.to(device) for name, tensor in inputs.items()}
    
    # Get the model outputs
    with torch.no_grad():
        outputs = model(**inputs)
    
    # Get the probabilities with softmax
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    
    # Move probs back to CPU if necessary for further processing
    probs = probs.cpu().numpy()[0]
    
    return probs
