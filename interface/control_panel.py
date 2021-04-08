from tkinter import *
from V1A1_definitief.database import mongo_to_pg, PostgresDAO
from V1A1_definitief.recommendation_rules import popularity_norm_recommendation, freq_combined_sub_sub_category_recommendation
from V1A1_definitief.recommendation_rules.buying_power_recommendation import frequently_combined_proid_sub_sub_category
import threading


db_gen_is_running = False
sub_sub_is_running = False
frequently_combined_is_running = False


def start_mongo():
    global db_gen_is_running
    thread = threading.Thread(target=mongo_to_pg.start_mongo_to_pg, args=('../database/DDL1.txt',))
    if not db_gen_is_running:
        db_gen_is_running = True
        thread.start()


def start_sub_sub_recommendations():
    global sub_sub_is_running
    thread = threading.Thread(target=freq_combined_sub_sub_category_recommendation.sub_sub_recommendations, args=(PostgresDAO.db, "freq_combined", 4))
    if not sub_sub_is_running:
        sub_sub_is_running = True
        print("started sub_sub_recommendations")
        thread.start()


def start_frequently_combined():
    global frequently_combined_is_running
    thread = threading.Thread(target=frequently_combined_proid_sub_sub_category.frequently_combined, args=PostgresDAO.db)
    if not frequently_combined_is_running:
        frequently_combined_is_running = True
        print("started frequently_combined")
        thread.start()


def start_popularity():
    popularity_norm_recommendation.start_popularity_norm_recommendation()


def start_time_travel():
    time = (time_day.get(), time_month.get(), time_year.get())
    time = " ".join(time)
    day, month, year = map(int, time.split())
    popularity_norm_recommendation.start_time_travel(day, month, year)


root = Tk()
root.title("Control panel")
root.geometry("1028x512")

#Start_frame
start_frame = Frame(master=root, bg="#A9A9A9")
start_frame.place(relheight=1, relwidth=1)
label = Label(master=start_frame,
              text="Database Control panel",
              background="#A9A9A9",
              foreground="#696969",
              font=("Helvectica", 30, "bold"))
label.place(relx=0.25, rely=0, relheight=0.1, relwidth=0.5)

start_button = Button(master=start_frame, text="generate the database", command=start_mongo)
start_button.place(relx=0.375, rely=0.1, relheight=0.1, relwidth=0.25)


start_button = Button(master=start_frame, text="Travel through time", command=start_time_travel)
start_button.place(relx=0.125, rely=0.2, relheight=0.1, relwidth=0.25)

day_label = Label(master=start_frame, text="day")
day_label.place(relx=0.375, rely=0.2, relheight=0.05, relwidth=0.04)
time_day = Entry(master=start_frame)
time_day.place(relx=0.375, rely=0.25, relheight=0.05, relwidth=0.04)

day_label = Label(master=start_frame, text="month")
day_label.place(relx=0.425, rely=0.2, relheight=0.05, relwidth=0.04)
time_month = Entry(master=start_frame)
time_month.place(relx=0.425, rely=0.25, relheight=0.05, relwidth=0.04)

day_label = Label(master=start_frame, text="year")
day_label.place(relx=0.475, rely=0.2, relheight=0.05, relwidth=0.04)
time_year = Entry(master=start_frame)
time_year.place(relx=0.475, rely=0.25, relheight=0.05, relwidth=0.04)


start_button = Button(master=start_frame, text="start frequently combined", command=start_popularity)
start_button.place(relx=0.125, rely=0.3, relheight=0.1, relwidth=0.25)


start_button = Button(master=start_frame, text="start sub sub recommendation", command=start_sub_sub_recommendations)
start_button.place(relx=0.125, rely=0.4, relheight=0.1, relwidth=0.25)


start_button = Button(master=start_frame, text="start frequently combined", command=start_frequently_combined)
start_button.place(relx=0.125, rely=0.5, relheight=0.1, relwidth=0.25)





root.mainloop()