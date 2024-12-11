from flask import Flask, request, jsonify, render_template
import openai
import re

app = Flask(__name__)

openai.api_type = "azure"
openai.api_base = "https://openai-innovation-dev.openai.azure.com/"
openai.api_version = "2024-02-01"
openai.api_key = "CwJLrfhVQJwiOvmpNrfNFFSeRma2yoPIm5cJ89zPYAN3jBL3rfokJQQJ99AKACLArgHXJ3w3AAABACOGhVpM"

# Dictionary to store conversation context for each session
session_context = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    data = request.get_json()
    user_input = data['question']
    session_id = data.get('session_id', 'default')

    # Initialize context for new session
    if session_id not in session_context:
        session_context[session_id] = [
            {"role": "system", "content": "You are an AI assistant that helps people find information about medical conditions."}
        ]

    # Request the AI to identify the diagnosis
    diagnosis = identify_diagnosis(user_input)

    if not diagnosis:
        return jsonify({'answer': "Sorry, I couldn't identify a specific diagnosis in your question."})

    # Add a system message to clarify the user query
    clarification_message = f"You are looking for information about '{diagnosis}'."

    # Add the prompt to session context to fetch symptoms for the identified diagnosis
    prompt = f"What are the common symptoms of '{diagnosis}'?"
    session_context[session_id].append({"role": "user", "content": prompt})

    # Request response from OpenAI for symptoms of the identified diagnosis
    response = openai.ChatCompletion.create(
        engine="GPT4",
        messages=session_context[session_id],
        temperature=0.7,
        max_tokens=800,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    # Extract response text and add to context
    ai_response = response['choices'][0]['message']['content']
    session_context[session_id].append({"role": "assistant", "content": ai_response})

    # Clean up the response to remove unwanted phrases
    cleaned_response = ai_response.replace("The medical condition being referred to is", "")
    cleaned_response = cleaned_response.replace(f"{diagnosis}.", f"{diagnosis}:").strip()

    # Extract symptoms and display them
    bold_items = extract_bold_items(cleaned_response)

    if bold_items:
        final_response = f"You are looking for information about '{diagnosis}'.\nHere are some symptoms:\n"
        final_response += "\n".join([f"**{item}**" for item in bold_items])  # Symptoms listed one per line
    else:
        final_response = f"{clarification_message}\nNo symptoms found."

    return jsonify({'answer': final_response})




def identify_diagnosis(question):
    # Ask the AI to identify just the medical condition from the user's question
    prompt = f"From the following question, extract the medical condition being referred to: '{question}'"
    response = openai.ChatCompletion.create(
        engine="GPT4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5,
        max_tokens=100
    )
    diagnosis = response['choices'][0]['message']['content'].strip()
    
    # Check if the diagnosis is a valid medical condition based on AI extraction
    if diagnosis:
        return diagnosis
    return None

def extract_bold_items(response):
    # Extract bolded items (symptoms) enclosed in '**' in markdown syntax
    bold_items = re.findall(r'\*\*(.*?)\*\*', response)
    return bold_items if bold_items else []

if __name__ == '__main__':
    app.run(debug=True)
