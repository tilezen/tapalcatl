Tapalcatl
=========

Tapalcatl is a "metatile server", which takes requests for a specific single format and tile coordinate and makes a request upstream for a "metatile" or bundle of tiles which contains the tile originally requested. 

_There is also a Python version optimized for Lambda functions in [tapalcatl-py](https://github.com/tilezen/tapalcatl-py)._

For example:

1. The client requests a tile `/1/2/3.json`
2. Tapalcatl makes an upstream request for `/1/0/0.zip`
3. Tapalcatl extracts the file `0/2/3.json` from the package.
4. The client reads back a tile containing only the format for the specific tile they asked for.

Why?
----

Having a choice of different formats can be helpful to support a range of different renderers and for ingesting tiles into different bits of software. Some formats are widely supported, such as [GeoJSON](http://geojson.org/), but can be less compact than other formats such as [Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec). Allowing users to make the choice means they can make that trade-off for themselves.

On the other hand, keeping a large set of different formats on disk means many tiny files to write, synchronise and manage. This can quickly become onerous and expensive!

Tapalcatl is an attempt to do this at the "edge", closer to the client and taking advantage of as much "edge" caching as possible.

Why "tapalcatl"?
---------------

[Tapalcatl](https://en.wiktionary.org/wiki/tapalcatl) is Nahuatl for a [potsherd](https://en.wiktionary.org/wiki/potsherd) or broken tile, and Tapalcatl breaks up (meta)tiles. It's also sibling to [Xonacatl](https://github.com/tilezen/xonacatl) which serves layers.

Installing
----------

```
cd $GOPATH
go get -u github.com/tilezen/tapalcatl/tapalcatl_server
go install github.com/tilezen/tapalcatl/tapalcatl_server
```
