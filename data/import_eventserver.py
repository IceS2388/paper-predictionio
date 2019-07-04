"""
Import sample data for recommendation engine
"""

import predictionio
import argparse
import time

RATE_ACTIONS_DELIMITER = ","


def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print("Importing data...")

  for line in f:
    data = line.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
    # For demonstration purpose, randomly mix in some buy events

    client.create_event(
        event="rate",
        entity_type="user",
        entity_id=data[0],
        target_entity_type="item",
        target_entity_id=data[1],
        properties= {
          "rating" : float(data[2]),
          "timestamp": time.localtime(data[3])
                      }
    )

    count += 1
  f.close()
  print("%s events are imported." % count)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import movielens data for recommendation engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/movielens/ratings.csv")

  args = parser.parse_args()
  print(args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
