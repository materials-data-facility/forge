def parse(arg):
    for i in range(10):
        yield {
            "composition": "NO NoNe" + str(i**2),
            "tag": "atom" if i % 2 else "molecule",
            "desc": "This is an example" + " of an example"*i,
            "url": "https://materialsdatafacility.org/#example#" + str(i),
            "id": str(i*2) + "abc",
            "useful": "insert useful data here"
            }

if __name__ == "__main__":
    print("This is an example of a parser. It does not do anything useful, but real parsers do.")
    print("USAGE:")
    print("parse(arg)")
    print("Pass any argument at all to parse(). It doesn't matter.")

