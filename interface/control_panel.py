from tkinter import *
from V1A1_definitief.database import mongo_to_pg, PostgresDAO
from V1A1_definitief.recommendation_rules import \
    popularity_norm_recommendation, freq_combined_sub_sub_category_recommendation,\
    propositional_logic_recommendation
from V1A1_definitief.recommendation_rules.buying_power_recommendation import frequently_combined_proid_sub_sub_category
import threading


db_gen_is_running = False
sub_sub_is_running = False
frequently_combined_is_running = False
propositional_logic_is_running = False


def start_mongo():
    """This function is being called by the ui,
     it starts the mongo_to_pg function in a separate thread"""
    global db_gen_is_running
    thread = threading.Thread(target=mongo_to_pg.start_mongo_to_pg, args=('../database/DDL1.txt',))
    if not db_gen_is_running:
        db_gen_is_running = True
        thread.start()
    return thread


def start_sub_sub_recommendations():
    """This function is being called by the ui,
     it starts the sub_sub_recommendations function in a separate thread"""
    global sub_sub_is_running
    thread = threading.Thread(target=freq_combined_sub_sub_category_recommendation.sub_sub_recommendations, args=(PostgresDAO.db, "freq_combined", 4))
    if not sub_sub_is_running:
        sub_sub_is_running = True
        print("started sub_sub_recommendations")
        thread.start()
    return thread


def start_frequently_combined():
    """This function is being called by the ui,
     it starts the frequently_combined function in a separate thread"""
    global frequently_combined_is_running
    thread = threading.Thread(target=frequently_combined_proid_sub_sub_category.start_frequently_combined)
    if not frequently_combined_is_running:
        frequently_combined_is_running = True
        print("started frequently_combined")
        thread.start()
    return thread


def start_propositional_logic():
    """This function is being called by the ui,
     it starts the propositional_logic function in a separate thread"""
    global propositional_logic_is_running
    thread = threading.Thread(target=propositional_logic_recommendation.start_propositional_logic_recommendation)
    if not propositional_logic_is_running:
        propositional_logic_is_running = True
        print("started propositional_logic")
        thread.start()


def start_determine_user_price_preference():
    pass


def start_subsubcat_price_information():
    pass


def start_popularity():
    """This function is being called by the ui,
     it starts the popularity_norm_recommendation"""
    popularity_norm_recommendation.start_popularity_norm_recommendation()


def start_time_travel():
    """This function is being called by the ui,
    it takes 3 inputs in the form of ui input (day, month, year) and gives this data to the time_travel function"""
    time = (time_day.get(), time_month.get(), time_year.get())
    time = " ".join(time)
    day, month, year = map(int, time.split())
    popularity_norm_recommendation.start_time_travel(day, month, year)


def start_generate_all():
    """this function is being called by the ui to start the generate_all function
    it starts the generate_all function in a separate thread"""
    thread = threading.Thread(target=generate_all)
    if not (db_gen_is_running or sub_sub_is_running or frequently_combined_is_running):
        thread.start()


def generate_all():
    """this function calls all the database and recommendation functions to generate a complete database,
    this database contains all the tabels and data that is used in this project.
    It starts by calling the start_mongo function and waits until this is done to start on the next function.
    the start_frequently_combined, start_popularity and the start_propositional_logic functions
     are being called to run at the same time.
    The function waits till the start_frequently_combined function is done to start on the
     start_sub_sub_recommendation function.
    """
    start_mongo().join()
    start_frequently_combined().join()
    start_popularity()
    start_propositional_logic()
    start_sub_sub_recommendations().join()
    print("finished generating everything.")
    

"""creates the UI for the control panel"""
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


#generates the database from mongo button
start_button = Button(master=start_frame, text="generate the database from mongo", command=start_mongo)
start_button.place(relx=0.375, rely=0.1, relheight=0.09, relwidth=0.25)


#start sub sub recommendation button
start_button = Button(master=start_frame, text="start sub sub recommendation", command=start_sub_sub_recommendations)
start_button.place(relx=0.125, rely=0.2, relheight=0.09, relwidth=0.25)


#start frequently combined button
start_button = Button(master=start_frame, text="start frequently combined", command=start_frequently_combined)
start_button.place(relx=0.125, rely=0.3, relheight=0.09, relwidth=0.25)


#start start propositional logic button
start_button = Button(master=start_frame, text="start propositional logic", command=start_propositional_logic)
start_button.place(relx=0.125, rely=0.4, relheight=0.09, relwidth=0.25)


#start determine user price preference button
start_button = Button(master=start_frame, text="start determine user price preference", command=start_determine_user_price_preference)
start_button.place(relx=0.125, rely=0.5, relheight=0.09, relwidth=0.25)

#start subsubcat_price_info button
start_button = Button(master=start_frame, text="start subsubcat_price_info", command=start_subsubcat_price_information)
start_button.place(relx=0.125, rely=0.6, relheight=0.09, relwidth=0.25)


#start popularity button
start_button = Button(master=start_frame, text="start popularity", command=start_popularity)
start_button.place(relx=0.125, rely=0.7, relheight=0.09, relwidth=0.25)


#Travel through time button
start_button = Button(master=start_frame, text="Travel through time", command=start_time_travel)
start_button.place(relx=0.125, rely=0.8, relheight=0.09, relwidth=0.25)

#creates labels and input for day, month and year for the time travel function
day_label = Label(master=start_frame, text="day")
day_label.place(relx=0.375, rely=0.8, relheight=0.04, relwidth=0.04)
time_day = Entry(master=start_frame)
time_day.place(relx=0.375, rely=0.85, relheight=0.04, relwidth=0.04)

day_label = Label(master=start_frame, text="month")
day_label.place(relx=0.425, rely=0.8, relheight=0.04, relwidth=0.04)
time_month = Entry(master=start_frame)
time_month.place(relx=0.425, rely=0.85, relheight=0.04, relwidth=0.04)

day_label = Label(master=start_frame, text="year")
day_label.place(relx=0.475, rely=0.8, relheight=0.04, relwidth=0.04)
time_year = Entry(master=start_frame)
time_year.place(relx=0.475, rely=0.85, relheight=0.04, relwidth=0.04)


#generate everything button
start_button = Button(master=start_frame, text="generate everything", command=start_generate_all)
start_button.place(relx=0.375, rely=0.9, relheight=0.09, relwidth=0.25)


root.mainloop()