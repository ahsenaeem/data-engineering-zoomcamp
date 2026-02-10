from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue

    print(f"\t- {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        if len(content) > 0: 
            print(f'\t Content: {content}')
        else:
            print(f'\t No content exists in {filepath}')
