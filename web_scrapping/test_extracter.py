import re


with open("results/raw_markdowns.md", "r") as f:
    text = f.read()

pattern = r'https?://[^\s)]+'

links = re.findall(pattern, text)
print(links)