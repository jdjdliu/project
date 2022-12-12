import base64

fname = "1.txt"


def b():
    fin = open(f"{fname}", "rb")
    fout = open(f"{fname}_b", "w")
    t = base64.encodebytes(fin.read())
    fout.write(t.decode())
    fin.close()
    fout.close()


def db():
    fin = open(f"{fname}", "rb")
    fout = open(f"{fname}_c", "wb")
    t = base64.decodebytes(fin.read())
    fout.write(t)
    fin.close()
    fin.close()


db()