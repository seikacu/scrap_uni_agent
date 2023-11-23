from bs4 import BeautifulSoup


def get_soup(full_path):
    if full_path.endswith(".htm"):
        with open(full_path, encoding="utf-8") as file:
            src = file.read()
        soup = BeautifulSoup(src, "lxml")
        return soup
