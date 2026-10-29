import speech_recognition as sr
import pyttsx3
import webbrowser
import os
import subprocess
import random

# ── Setup ──────────────────────────────────────────────
engine = pyttsx3.init()
engine.setProperty('rate', 160)
engine.setProperty('volume', 1.0)

# ── Female Voice Selection ─────────────────────────────
voices = engine.getProperty('voices')
female_voice_set = False
for voice in voices:
    if any(word in voice.name.lower() for word in ['female', 'woman', 'zira', 'hazel', 'susan', 'catherine', 'karen', 'victoria', 'samantha']):
        engine.setProperty('voice', voice.id)
        female_voice_set = True
        break

if not female_voice_set:
    # Fallback: on Windows, index 1 is usually female (Zira)
    if len(voices) > 1:
        engine.setProperty('voice', voices[1].id)

recognizer = sr.Recognizer()

# ── Multiple Responses Per Command ─────────────────────
RESPONSES = {

    "open_browser": [
        "Yes Sir, opening your browser right away!",
        "Of course Sir, launching the browser for you.",
        "Right away Sir, your browser is on its way!",
        "Sure thing Sir, opening the browser now.",
        "Absolutely Sir, bringing up the browser!",
    ],

    "open_youtube": [
        "Yes Sir, taking you to YouTube right now!",
        "Of course Sir, opening YouTube for you!",
        "Sure Sir, YouTube coming right up!",
        "Right away Sir, launching YouTube!",
        "Absolutely, heading to YouTube now Sir!",
    ],

    "search_youtube": [
        "Yes Sir, searching YouTube for {query} now!",
        "Of course Sir, looking up {query} on YouTube right away!",
        "Sure thing Sir, finding {query} on YouTube for you!",
        "Right away Sir, searching for {query} on YouTube!",
        "On it Sir, pulling up {query} on YouTube!",
    ],

    "search_google": [
        "Yes Sir, searching Google for {query} now!",
        "Of course Sir, Googling {query} right away!",
        "Sure thing Sir, looking up {query} on Google!",
        "Right away Sir, searching for {query} on Google!",
        "On it Sir, finding {query} on Google for you!",
    ],

    "open_explorer": [
        "Yes Sir, opening File Explorer for you!",
        "Of course Sir, launching File Explorer right away!",
        "Sure thing Sir, bringing up File Explorer!",
        "Right away Sir, opening File Explorer now!",
        "Absolutely Sir, File Explorer is on its way!",
    ],

    "open_notepad": [
        "Yes Sir, opening Notepad for you!",
        "Of course Sir, launching Notepad right away!",
        "Sure thing Sir, bringing up Notepad now!",
        "Right away Sir, Notepad is on its way!",
    ],

    "open_calculator": [
        "Yes Sir, opening the Calculator for you!",
        "Of course Sir, launching the Calculator right away!",
        "Sure Sir, bringing up the Calculator now!",
        "Right away Sir, the Calculator is coming up!",
    ],

    "what_time": [
        "Sir, the current time is {time}.",
        "It is {time} right now Sir.",
        "The time is {time} Sir.",
        "Currently it is {time} Sir.",
    ],

    "what_date": [
        "Sir, today is {date}.",
        "Today's date is {date} Sir.",
        "It is {date} today Sir.",
    ],

    "greet_morning": [
        "Good morning Sir! I hope you slept well. How can I help you today?",
        "Good morning Sir! Ready to assist you. What would you like me to do?",
        "A very good morning to you Sir! I am at your service.",
    ],

    "greet_hello": [
        "Hello Sir! How can I assist you today?",
        "Hello Sir! I am ready. What do you need?",
        "Hi Sir! Great to hear from you. How can I help?",
        "Hello Sir! Your assistant is listening.",
    ],

    "how_are_you": [
        "I am doing very well Sir, thank you for asking! How may I help you?",
        "I am functioning perfectly Sir! Ready to assist you.",
        "All systems running smoothly Sir! What can I do for you?",
        "I am great Sir, always happy to help you!",
    ],

    "your_name": [
        "I am your personal virtual assistant Sir. You can call me Nova.",
        "My name is Nova, your personal assistant Sir.",
        "I go by Nova Sir, here to serve you!",
    ],

    "thank_you": [
        "You are most welcome Sir!",
        "Happy to help Sir, anytime!",
        "My pleasure Sir!",
        "Always here for you Sir!",
        "Glad I could help Sir!",
    ],

    "joke": [
        "Why do programmers prefer dark mode? Because light attracts bugs, Sir!",
        "Why did the computer go to the doctor? Because it had a virus, Sir!",
        "I would tell you a joke about Wi-Fi, Sir, but I know you would not connect with it.",
        "Why was the math book sad, Sir? Because it had too many problems!",
    ],

    "goodbye": [
        "Goodbye Sir! Have a wonderful day!",
        "See you later Sir! Take care!",
        "Farewell Sir! It was a pleasure assisting you!",
        "Goodbye Sir! Come back soon!",
    ],

    "unknown": [
        "I am sorry Sir, I did not understand that. Could you try again?",
        "Hmm, I am not sure how to do that yet Sir. More commands are coming soon!",
        "I apologize Sir, that command is not in my skills yet.",
        "I did not quite catch that Sir. Could you rephrase it?",
    ],
}

def respond(key, **kwargs):
    """Pick a random response for the given key and fill in any placeholders."""
    template = random.choice(RESPONSES[key])
    return template.format(**kwargs)

# ── Speak & Listen ─────────────────────────────────────
def speak(text):
    print(f"[Assistant]: {text}")
    engine.say(text)
    engine.runAndWait()

def listen():
    with sr.Microphone() as source:
        print("\n[Listening...]")
        recognizer.adjust_for_ambient_noise(source, duration=0.5)
        try:
            audio = recognizer.listen(source, timeout=5)
            command = recognizer.recognize_google(audio)
            print(f"[You said]: {command}")
            return command.lower()
        except sr.WaitTimeoutError:
            return ""
        except sr.UnknownValueError:
            speak("Sorry Sir, I did not catch that. Please try again.")
            return ""
        except sr.RequestError:
            speak("I cannot reach the speech service right now Sir.")
            return ""

# ── Commands ───────────────────────────────────────────
def handle_command(command):
    import datetime

    # ── Greetings ──
    if any(w in command for w in ["good morning", "morning"]):
        speak(respond("greet_morning"))

    elif any(w in command for w in ["hello", "hi", "hey"]):
        speak(respond("greet_hello"))

    elif any(w in command for w in ["how are you", "how are you doing", "are you okay"]):
        speak(respond("how_are_you"))

    elif any(w in command for w in ["what is your name", "who are you", "your name"]):
        speak(respond("your_name"))

    elif any(w in command for w in ["thank you", "thanks", "thank u"]):
        speak(respond("thank_you"))

    elif any(w in command for w in ["tell me a joke", "say a joke", "joke"]):
        speak(respond("joke"))

    # ── Time & Date ──
    elif any(w in command for w in ["what time", "current time", "tell me the time"]):
        now = datetime.datetime.now().strftime("%I:%M %p")
        speak(respond("what_time", time=now))

    elif any(w in command for w in ["what date", "today's date", "what is today", "current date"]):
        today = datetime.datetime.now().strftime("%A, %B %d, %Y")
        speak(respond("what_date", date=today))

    # ── Apps ──
    elif "open file explorer" in command or "open explorer" in command:
        speak(respond("open_explorer"))
        subprocess.Popen("explorer")

    elif "open notepad" in command:
        speak(respond("open_notepad"))
        subprocess.Popen("notepad")

    elif "open calculator" in command or "open calc" in command:
        speak(respond("open_calculator"))
        subprocess.Popen("calc")

    elif "open browser" in command or "open chrome" in command or "open edge" in command:
        speak(respond("open_browser"))
        webbrowser.open("https://www.google.com")

    # ── YouTube ──
    elif "open youtube" in command or "go to youtube" in command:
        speak(respond("open_youtube"))
        webbrowser.open("https://www.youtube.com")

    elif "search youtube" in command:
        query = command.replace("search youtube for", "").replace("search youtube", "").strip()
        if query:
            speak(respond("search_youtube", query=query))
            webbrowser.open(f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}")
        else:
            speak("What would you like me to search for on YouTube Sir?")

    # ── Google ──
    elif "search" in command and "google" in command:
        query = command.replace("search google for", "").replace("search google", "").replace("google", "").strip()
        speak(respond("search_google", query=query))
        webbrowser.open(f"https://www.google.com/search?q={query.replace(' ', '+')}")

    # ── Exit ──
    elif any(w in command for w in ["stop", "exit", "quit", "goodbye", "bye"]):
        speak(respond("goodbye"))
        return False

    else:
        speak(respond("unknown"))

    return True

# ── Main ───────────────────────────────────────────────
def main():
    speak("Hello Sir! I am Nora fatehi, your personal virtual assistant. How may I help you today?")
    running = True
    while running:
        command = listen()
        if command:
            running = handle_command(command)

if __name__ == "__main__":
    main()