import json
import os

class Translator:
    def __init__(self, lang='en'):
        self.lang = lang
        self._strings = {}
        
        try:
            # Path to the locales/strings.json file
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            locales_path = os.path.join(script_dir, 'locales', 'strings.json')
            with open(locales_path, 'r', encoding='utf-8') as f:
                self._strings = json.load(f)
        except Exception as e:
            print(f"Could not load translation file: {e}")

    def get(self, key, **kwargs):
        template = self._strings.get(key, {}).get(self.lang, f"[{key}]")
        try:
            return template.format(**kwargs)
        except KeyError as e:
            return f"[Missing format key {e} for '{key}']"

# Create a global instance for easy access
t = Translator()