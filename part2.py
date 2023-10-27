from Database import Database
from datetime import datetime, timedelta
from haversine import haversine, Unit
from pprint import pprint
from icecream import ic # For debugging

def q1(database : Database) -> None:
    db = database.db
    print(f'Question 1\n')
    print(f'Number of documents in User: {db.User.count_documents({})}')
    print(f'Number of documents in Activity: {db.Activity.count_documents({})}')
    print(f'Number of documents in TrackPoint: {db.TrackPoint.count_documents({})}')
    print()

def q2(database : Database) -> None:
    db = database.db
    results = db.User.aggregate([
        {
            '$project': {'actcvities': {'$size': '$activities'}}
        },
        {
            '$group': {'_id': None, 'avgActivities': {'$avg': '$activities'}}
        }
    ])
    print('Question 2\n')
    print(f'Average number of activities per user: {list(results)[0]['avgActivities']}')
    print()

def q3(database : Database) -> None:
    db = database.db
    results = db.User.aggregate([
        {
            '$project': {'numActivities': {'$size': '$activities'}}
        },
        {
            '$group': 
            {
                '_id': None, 
                'topUsers':
                {
                    '$topN': 
                    {
                        'n': 20,
                        'sortBy': {'numActivities': -1},
                        'output': '$_id'
                    }
                }
            }
        }
    ])
    print('Question 3\n')
    print('Top users by number of activities:')
    pprint(list(results)[0]['topUsers'])
    print()

def q4(database : Database) -> None:
    db = database.db
    results = db.Activity.aggregate([
        {
            '$match': {'transportation_mode': 'taxi'}
        },
        {
            '$group': {'_id': None, 'unique_users': {'$addToSet': '$user'}}
        }
    ])
    print('Question 4\n')
    print('Users that have used a taxi:')
    pprint(list(results)[0]['unique_users'])
    print()

def q5(database : Database) -> None:
    db = database.db
    results = db.Activity.aggregate([
        {
            '$match': 
            {
                'transportation_mode': 
                {
                    '$not': {'$eq': None}
                 }
            }
        },
        {
            '$group':
            {
                '_id': '$transportation_mode',
                'activity_count': {'$sum': 1}
            }
        }
    ])
    print('Question 5\n')
    print('Transportation modes and number of activities:')
    pprint(list(results))
    print()

def q6a(database : Database) -> None:
    db = database.db
    results = db.Activity.aggregate([
        {
            '$group': 
            {
                '_id': {'$year': '$start_date_time'},
                'count': {'$sum': 1}
            }
        },
        {
            '$group': 
            {
                '_id': None,
                'topYear':
                {
                    '$top':
                    {
                        'sortBy': {'count': -1},
                        'output': '$_id'
                    }
                }
            }
        }
    ])
    print('Question 6(a):\n')
    print('Year with the greatest number of activities:')
    pprint(list(results)[0]['topYear'])
    print()

def q6b(database : Database) -> None:
    db = database.db
    results = db.Activity.aggregate([
        {
            '$group': 
            {
                '_id': {'$year': '$start_date_time'},
                'hours': 
                {
                    '$sum': 
                    {
                        '$subtract': ['$end_date_time', '$start_date_time']
                    }
                }
            }
        },
        {
            '$group': 
            {
                '_id': None,
                'topYear':
                {
                    '$top':
                    {
                        'sortBy': {'hours': -1},
                        'output': '$_id'
                    }
                }
            }
        }
    ])
    print('Question 6(b):\n')
    print('Year with the greatest number of hours recorded:')
    pprint(list(results)[0]['topYear'])
    print()

def q7(database : Database) -> None:
    db = database.db
    activities = db.Activity.find({'user': '112', 'transportation_mode': 'walk'})
    distance_walked = 0
    for activity in activities:
        trackpoints = db.TrackPoint.find({'_id': {'$in': activity['trackpoints']},
                                           'date_time': 
                                           {
                                               '$gte': datetime(2008, 4, 1),
                                                '$lt': datetime(2009, 1, 1)
                                           }
                                        })

        prev_latlon = None
        for trackpoint in trackpoints:
            lat, lon = float(trackpoint['lat']), float(trackpoint['lon'])

            if not prev_latlon:
                prev_latlon = (lat, lon)
                continue

            dist = haversine(prev_latlon, (lat, lon), unit=Unit.KILOMETERS)
            distance_walked += dist
            prev_latlon = (lat, lon)
    
    print('Question 7:\n')
    print('Total distance walked by user 112 (kilometers):')
    pprint(distance_walked)
    print()

def q8(database : Database) -> None:
    db = database.db
    trackpoints = db.TrackPoint.find({'altitude': {'$ne': '-777'}}).sort([('activity.user', 1), ('activity._id', 1), ('_id', 1)])

    users = {}
    prev_alt = None
    prev_act = None
    prev_user = None
    for trackpoint in trackpoints:
        cur_alt = float(trackpoint['altitude'])
        cur_act = trackpoint['activity']['_id']
        cur_user = trackpoint['activity']['user']
        if not prev_alt:
            prev_alt = cur_alt
            prev_act = cur_act
            continue

        if prev_act != cur_act:
            prev_alt = cur_alt
            prev_act = cur_act
            continue

        if prev_user != cur_user:
            users[cur_user] = 0

        if prev_alt < cur_alt:
            users[cur_user] += cur_alt - prev_alt
        
        prev_alt = cur_alt
        prev_act = cur_act
        prev_user = cur_user
    
    results = []
    for key, value in users.items():
        results.append({'user': key, 'gained': int(value*0.3048)})

    results = sorted(results, key=lambda item: item['gained'], reverse=True)

    print('Question 8:\n')
    print('Top 20 users who have gained the most altitude in meters (gross):')
    pprint(results[:20])
    print()

def q9(database : Database) -> None:
    db = database.db
    activities = db.Activity.find({})

    results = {}

    for activity in activities:
        trackpoints = db.TrackPoint.find({'_id': {'$in': activity['trackpoints']}}).sort('_id', 1)

        prev_datetime = None
        for trackpoint in trackpoints:
            cur_datetime = trackpoint['date_time']
            if prev_datetime is None:
                prev_datetime = cur_datetime
                continue

            if (cur_datetime - prev_datetime) > timedelta(minutes=5):
                if trackpoint['activity']['user'] not in results:
                    results[trackpoint['activity']['user']] = 1
                else:
                    results[trackpoint['activity']['user']] += 1
                break
            prev_datetime = cur_datetime
    print('Question 9:\n')
    print('Users that have invalid activities and the number of invalid activities per user:')
    pprint(results)
    print()

def q10(database : Database) -> None:
    db = database.db
    trackpoints = db.TrackPoint.find({})

    CITY_COORDS = (39.916, 116.397)

    results = {}

    for trackpoint in trackpoints:
        user = trackpoint['activity']['user']
        if user in results:
            continue

        latlon = float(trackpoint['lat']), float(trackpoint['lon'])
        dist = haversine(CITY_COORDS, latlon, unit=Unit.METERS)
        if dist <= 1000:
            results[user] = True
    
    users = [key for key in results]
    print('Question 10:\n')
    print('Users that have been to the forbidden city (within 1000 meters of center):')
    pprint(users)
    print()

def q11(database : Database) -> None:
    db = database.db
    results = db.Activity.aggregate([
        {
            '$match': {'transportation_mode': {'$ne': None}}
        },
        {
            '$group': 
            {
                '_id': {'user': '$user', 'transportation_mode': '$transportation_mode'},
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': 
            {
                '_id.user': 1,
                'count': -1
            }
        },
        {
            '$group':
            {
                '_id': {'user': '$_id.user'},
                'transportation_mode': {'$first': '$_id.transportation_mode'},
            }
        },
        {
            '$sort': {'_id.user': 1}
        }
    ])

    results = [(i['_id']['user'], i['transportation_mode']) for i in results]
    print('Question 11:\n')
    print('Most used transportation mode by user:')
    pprint(list(results))
    print()

if __name__ == '__main__':
    db = Database()
    q1(db)
    q2(db)
    q3(db)
    q4(db)
    q5(db)
    q6a(db)
    q6b(db)
    q7(db)
    q8(db)
    q9(db)
    q10(db)
    q11(db)