from tkinter import *
from V1A1_definitief.database import mongo_to_pg
from V1A1_definitief.recommendation_rules import popularity_norm_recommendation
import threading


db_gen_is_running = False

def start_mongo():
    global db_gen_is_running
    thread = threading.Thread(target=mongo_to_pg.start_mongo_to_pg, args=('../database/DDL1.txt',))
    if not db_gen_is_running:
        db_gen_is_running = True
        thread.start()

def start_popularity():
    popularity_norm_recommendation.start_popularity_norm_recommendation()

def start_time_travel():
    time = time_input.get()
    day,month,year = map(int, time.split())
    print(day, month, year)
    print(type(day))
    popularity_norm_recommendation.start_time_travel(day, month, year)


root = Tk()
root.title("root title")
root.geometry("1028x512")

#Start_frame
start_frame = Frame(master=root, bg="#A9A9A9")
start_frame.place(relheight=1, relwidth=1)
label = Label(master=start_frame,
              text="start text",
              background="#A9A9A9",
              foreground="#696969",
              font=("Helvectica", 30, "bold"))
label.place(relx=0.25, rely=0, relheight=0.1, relwidth=0.5)

start_button = Button(master=start_frame, text="button text start mongo", command=start_mongo)
start_button.place(relx=0.25, rely=0.6, relheight=0.1, relwidth=0.25)

start_button = Button(master=start_frame, text="button text start popularity recommendation", command=start_popularity)
start_button.place(relx=0.25, rely=0.5, relheight=0.1, relwidth=0.25)

start_button = Button(master=start_frame, text="Travel through time", command=start_time_travel)
start_button.place(relx=0.25, rely=0.4, relheight=0.1, relwidth=0.25)

time_input = Entry(master=start_frame)
time_input.place(relx=0.5, rely=0.4, relheight=0.1, relwidth=0.25)







root.mainloop()