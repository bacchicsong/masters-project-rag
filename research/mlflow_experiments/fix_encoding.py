"""Fix emoji characters that break Windows terminal (cp1252)."""
import os
import re
import glob

EMOJI_MAP = {
    '\u2705': '[OK]',
    '\u274c': '[FAIL]',
    '\u26a0\ufe0f': '[WARN]',
    '\u26a0': '[WARN]',
    '\U0001f4e6': '[PACKAGE]',
    '\U0001f50c': '[SEARCH]',
    '\U0001f4ca': '[CHART]',
    '\U0001f52c': '[EXP]',
    '\U0001f4d0': '[RESULTS]',
    '\U0001f4ac': '[TEST]',
    '\U0001f3af': '[TARGET]',
    '\U0001f504': '[RETRY]',
    '\U0001f680': '[START]',
    '\u2795': '[ADD]',
    '\u2796': '[REMOVE]',
    '\u2714\ufe0f': '[DONE]',
    '\u2714': '[DONE]',
    '\U0001f4a1': '[INFO]',
    '\U0001f4dd': '[NOTE]',
    '\U0001f3c6': '[AWARD]',
    '\u2139\ufe0f': '[INFO]',
    '\u2139': '[INFO]',
    '\U0001f447': '[BELOW]',
    '\U0001f4cb': '[LIST]',
    '\U0001f5c4\ufe0f': '[FOLDER]',
    '\U0001f4c4': '[FILE]',
    '\U0001f4c2': '[DIR]',
    '\U0001f511': '[KEY]',
    '\U0001f512': '[LOCK]',
    '\u23f3': '[WAIT]',
    '\u231b': '[WAIT]',
    '\u26a1': '[FAST]',
    '\U0001f50d': '[MAG]',
    '\U0001f50e': '[MAG+]',
    '\U0001f4a0': '[SYM]',
    '\U0001f4f6': '[NET]',
    '\U0001f310': '[GLOBE]',
}

def fix_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    before = content
    for emoji, replacement in EMOJI_MAP.items():
        content = content.replace(emoji, replacement)

    # Also fix common emoji sequences with \ufe0f variant selector
    content = re.sub(r'[\U0001F300-\U0001F9FF]\ufe0f?', lambda m: _emoji_or_keep(m.group(0)), content)

    if content != before:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def _emoji_or_keep(char):
    """Replace emoji if we have it, otherwise keep."""
    for emoji, replacement in EMOJI_MAP.items():
        if emoji in char:
            return replacement
    # Unknown emoji - try to remove or replace
    return '[ICON]'

if __name__ == '__main__':
    base = os.path.dirname(os.path.abspath(__file__))
    patterns = [
        os.path.join(base, 'experiment_*.py'),
        os.path.join(base, 'run_all_experiments.py'),
        os.path.join(base, 'mlflow_config.py'),
        os.path.join(base, 'utils', '*.py'),
    ]
    fixed = []
    for pattern in patterns:
        for filepath in glob.glob(pattern):
            if fix_file(filepath):
                fixed.append(filepath)
                print(f"Fixed: {filepath}")
    if not fixed:
        print("No files needed fixing.")
    else:
        print(f"\nFixed {len(fixed)} files.")