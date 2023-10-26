from Database import Database
from pprint import pprint

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
            '$project': {'activities': {'$size': '$activities'}}
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

if __name__ == '__main__':
    db = Database()
    q1(db)
    q2(db)
    q3(db)