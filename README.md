# Installation

Initialize git submodules

```shell
$ git submodule init # todo: check correct
```

Install requirements:

```shell
$ pip install -r requirements.txt
```

gtfspy reuires networkx1.10, but that's way too old. Force the issue:

$ pip install --upgrade --force networkx

Memoizing distance in a snae way is important. Calculating if things are nearby 
once costs 0.000263 seconds per stop:

```
Runtime: 0:00:05.995081 seconds for 22820 stops
or 0:00:00.000263 seconds per stop 
```

(that was doing all the comparisons for a single stop)

That seems small, but every stop has to be checked against every other stop, 
which means this grows witht he square of the number of stops. Even for the 
22820 stops in the test data set, this would take 38 hours!

Instead, it makes more sense to do some kind of heuristic to limit things to 
the (relatively) nearby stops only. The most sane way to do this is based on the
geolocation data we've already got - aka, latitude/longitude. Convenently this 
allows us to put the stations into (relatively) evenly sized boxes. (Only 
relatively because the earth is a sphere and so the distance between longitude 
lines is not consistent as you go to higher latitudes. However, we're working 
at a relatively small scale and this should be okay as long as we're not 
looking at anything in the arctic or antarctic circles...)

Anyway, this gives us a cheap way to heuristically memoize what stations are 
near each other, as follows. For each stop, we transform the latitude and 
longitude into a "box id", then store the box ID and the station. A box ID is 
formed of 10 digits, in the form XXXXYYYY, where XXXX is the latitude to 
hundreths of a parallel plus 9000 (to account for positive/negative)
(eg, 12.34567 becomes 1023). The same applies for longitude, with the exception
that we add 180 to the merdian. So 12.3456 becomes 1923. These two values are
then merged to get a box ID: 10231923. A reference to this box ID and this 
station is then stored.

Later, when we want to find stations near the current station, we can look in 
the adjacent 8 boxes for nearby stations. As a note, when calculating adjacent 
boxes, we need to check for over and underflow.

Overall, this should get us at least 10 kilometers to any side of the station, 
which we can then filter down based on actual distances.