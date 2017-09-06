CHANGELOG
=========

v1.4.0
------
* Add tilejson handler
* Ensure it's safe to always close S3 response body

v1.3.0
------
* Clamp to tile requests to 0/0/0 (don't allow negative tile coordinates)

v1.2.2
------
* Fix bug when S3 response body is nil

v1.2.1
------
* Improve json logging
* Close s3 responses

v1.2.0
------
* Log as json
* Make tile size configurable on a per-pattern basis.
* Improve healthcheck
* Add ability to log expvars periodically

v1.1.0
------
* Use HTTP compliant timestamp format string. (See https://github.com/tilezen/tapalcatl/issues/18)

v1.0.0
------
* Initial stable release
