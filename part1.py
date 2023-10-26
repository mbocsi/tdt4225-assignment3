from Database import Database
from Schema import User, Activity, TrackPoint
from pathlib import Path
import os
from datetime import datetime
import logging

FORMAT = '%(asctime)s : %(levelname)s : %(message)s'
logging.basicConfig(filename='part1.log', filemode='w', level=logging.INFO, format=FORMAT)

def main() -> None:
    """
    Main script for inserting all the data into the database
    """
    global activity_counter
    global trackpoint_counter
    trackpoint_counter = 0
    activity_counter = 0

    LABELED_IDS = []
    with open(Path('.') / 'dataset' / 'labeled_ids.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            LABELED_IDS.append(line.replace('\n', ''))
    logging.debug(f"Labeled IDs: {LABELED_IDS}")

    # Create the collections
    db = Database()
    if not db.create_collection('User'):
        quit()
    if not db.create_collection("Activity"):
        quit()
    if not db.create_collection("TrackPoint"):
        quit()

    user_obj = None
    for root, dirs, files in os.walk("dataset/Data", topdown=True):
        match len(Path(root).parts):
            case 3: # User 
                user = Path(root).parts[2]
                user_obj = User(user, user in LABELED_IDS, activities=[])
            case 4: # Activity
                activities = []
                track_points = []
                for file in files:
                    if file[-3:] != 'plt':
                        continue

                    filename = file.split('.')[0]
                    plt = []

                    with open(Path(root) / file, 'r') as f:
                        plt = f.readlines()

                    if len(plt[6:]) > 2500: # Only insert activites with fewer than 2501 points
                        logging.debug(f'Skipped activity: {filename}! TOO BIG! (size={len(plt[6:])})')
                        continue

                    activity = Activity(activity_counter, user, trackpoints=[])                                 # Create the Activity document
                    activity_counter += 1

                    first_line = plt[6].split(',')
                    start_date = first_line[-2]
                    start_time = first_line[-1].replace('\n', '')
                    start_datetime = datetime.strptime(f'{start_date} {start_time}', '%Y-%m-%d %H:%M:%S', )     # Start datetime

                    last_line = plt[-1].split(',')
                    end_date = last_line[-2]
                    end_time = last_line[-1].replace('\n', '')
                    end_datetime = datetime.strptime(f'{end_date} {end_time}', '%Y-%m-%d %H:%M:%S')             # End datetime

                    transportation = None # Transportation mode
                    if user in LABELED_IDS:
                        with open(Path(*Path(root).parts[:3]) / 'labels.txt', 'r') as f:
                            labels = f.readlines()
                        for label in labels[1:]:
                            label = label.split()
                            label_start = datetime.strptime(f'{label[0]} {label[1]}', '%Y/%m/%d %H:%M:%S')
                            label_end = datetime.strptime(f'{label[2]} {label[3]}', '%Y/%m/%d %H:%M:%S', )
                            if label_start == start_datetime and label_end == end_datetime:
                                transportation = label[4]
                                break
                    
                    # Update the Activity document
                    activity['transportation_mode'] = transportation
                    activity['start_date_time'] = start_datetime
                    activity['end_date_time'] = end_datetime

                    for point in plt[6:]:
                        lat, lon, _, alt, days, date, time = point.split(',')
                        time = time.replace('\n', '')
                        point_datetime = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                        trackpoint = TrackPoint(id=trackpoint_counter,
                                                lat=lat,
                                                lon=lon,
                                                altitude=alt,
                                                date_days=days,
                                                date_time=point_datetime,
                                                activity=activity.denorm()) # Create TrackPoint document
                        activity['trackpoints'].append(trackpoint['_id'])   # Update the Activity document with trackpoints
                        track_points.append(trackpoint)                     # Insert trackpoint into list of trackpoints for insertion later
                        logging.debug(f"Created TrackPoint: {trackpoint}")
                        trackpoint_counter += 1
                    activities.append(activity)                             # Insert activity into list of activites for insertion later
                    user_obj['activities'].append(activity['_id'])
                    logging.debug(f"Created Activity: {activity}")

                # Insert all data associated with the current user into the database
                db.insert_trackpoints(track_points)
                db.insert_activities(activities)
                db.insert_user(user_obj)
                logging.info(f"Created User: {user_obj}")
                    
def dropall() -> None:
    """
    Script for resetting the database (Used during development)
    """
    db = Database()
    db.drop_collection("User")
    db.drop_collection("Activity")
    db.drop_collection("TrackPoint")

if __name__ == '__main__':
    main()
    # dropall()