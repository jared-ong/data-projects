from tkinter import *



window = Tk()
window.title("Welcome Jared")
lbl = Label(window, text="Hello")
lbl.grid(column=0, row=0)
window.geometry('350x200')

def clicked():
    lbl.configure(text="Button was clicked !!")

btn = Button(window, text="Click Me")
btn.grid(column=1, row=0)
btn = Button(window, text="Click Here", bg='yellow', fg='black', command=clicked)
btn.grid(column=1, row=0)
window.mainloop()
