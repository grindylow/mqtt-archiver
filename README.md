# mqtt-archiver and mqtt-historian

Connect to an MQTT broker and archive all or selected topics for later retrieval.

Retrieve these data via simple HTTP calls.

Does not require a database.

All messages are stored chronologically in collections of human-readable (text) files.


## Features

 * Optional compression achieves size efficiency similar to proprietary binary formats. [not implemented yet]
 * Easily Retrieve data and analyse them with any tool that can process plain text files.
 * Chronological data retrieval is fast (speed of file read operation).
 * Random access is potentially slow, but seldom a requirement.


## Operation

Run as a background task. Will archive all topics found on the local MQTT broker by default.

Configure alternative MQTT broker locations and data archive locations in mqtt-archiver.conf [n-i-y].
