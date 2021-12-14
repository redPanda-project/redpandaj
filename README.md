# redpandaj [![Java CI](https://github.com/redPanda-project/redpandaj/actions/workflows/maven.yml/badge.svg?branch=dev)](https://github.com/redPanda-project/redpandaj/actions/workflows/maven.yml) [![Build Status](https://travis-ci.org/redPanda-project/redpandaj.svg?branch=master)](https://travis-ci.org/redPanda-project/redpandaj) [![Coverage Status](https://coveralls.io/repos/github/redPanda-project/redpandaj/badge.svg?branch=master)](https://coveralls.io/github/redPanda-project/redpandaj?branch=master)

This is a rework of the redpanda app. The old system does not work on current versions of android due to the restrictive battery optimization.
The new idea is to use push notifications (firebase massaging) to notify the user of new messages and the app itself does not run in the background on mobile phones. Therefore, the routing of messages has to be done by other (full) nodes of the network.
This may lead to a certain centralization of the app which we try to minimize.
This new system is designed around a new flutter app we are developing.   

The idea for the routing of messages is based on Kademlia DHT and is inspired by the floodfill of the i2p network to provide anonymity.

The protocol will change to FlatBuffers.

## compile
An easy way to download and install redpanda is to run the build.sh, this is not very safe and will be changed in the future. Make sure you have maven, git and jdk installed.

```bash
wget https://raw.githubusercontent.com/redPanda-project/redpandaj/master/helpful/build.sh
chmod +x build.sh
./build.sh
```

## Statement from the founders
The restrictions by Google and Apple in reference to their mobile operating systems, i.e. Android and iOS, led us to pause the project in April '17, since it was clear that Google would change the behavior of background services. But since the direction in these limitations given mainly by android Oreo and Pie is now a bit more clear, we have new plans to develop completely new light clients for mobile devices (android/iOS) by shifting the main parts from the routing of messages to the full nodes.

In addition to the clarifications on the limitations by Google and Apple, the landscape of applications changed as well. At the moment there are already other applications that (now) satisfy certain important criteria, see WhatsApp or Threema. However, it is our belief that our project still serves a purpose as a decentralized alternative that goes beyond the emerging minimal industry standard. Therefore, the work on the project is ongoing but limited in scope. In particular, our efforts are split between working on the white paper and the application.

The android application is deprecated and was removed from the play store.


## Dependencies

- [JGraphT](https://github.com/jgrapht/jgrapht/blob/master/README.md#introduction) License: LGPL 2.1 http://www.gnu.org/licenses/lgpl-2.1.html or EPL http://www.eclipse.org/org/documents/epl-v20.php