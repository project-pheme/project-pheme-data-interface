**This documentation is automatically generated.**

**Output schemas only represent `data` and not the full output; see output examples and the JSend specification.**

# /api/event

    Content-Type: application/json

## POST


**Input Schema**
```json
{
    "additionalProperties": false,
    "properties": {
        "dataSources": {
            "items": {
                "additionalProperties": false,
                "properties": {
                    "twitter": {
                        "additionalProperties": false,
                        "properties": {
                            "chronologicalOrder": {
                                "type": "boolean"
                            },
                            "keywords": {
                                "type": "string"
                            },
                            "type": {
                                "pattern": "^Twitter$",
                                "type": "string"
                            }
                        },
                        "required": [
                            "type",
                            "keywords"
                        ],
                        "type": "object"
                    }
                },
                "required": [
                    "twitter"
                ],
                "type": "object"
            },
            "type": "array"
        },
        "description": {
            "type": "string"
        },
        "endCaptureDate": {
            "type": "string"
        },
        "name": {
            "type": "string"
        },
        "startCaptureDate": {
            "type": "string"
        },
        "type": {
            "pattern": "^(search)|(stream)$",
            "type": "string"
        }
    },
    "required": [
        "name",
        "description",
        "dataSources"
    ],
    "type": "object"
}
```



**Output Schema**
```json
null
```



**Notes**

POST a new event to Capture and add it to the ushahidi instance

* `name`: name of the data channel
* `description`: description of the data channel
* `startCaptureDate`: optional , "YYYY-mm-dd HH:MM:SS.mmm"
* `endCaptureDate`: optional, "YYYY-mm-dd HH:MM:SS.mmm"
* `dataSources`: array of object
    * `type`: set to `Twitter`
    * `keywords`: twitter search spec
    * `chronologicalOrder`: optional, default to true



<br>
<br>

# /api/event/capture

    Content-Type: application/json



<br>
<br>

# /api/event/capture/\(?P\<datachannel\_id\>\[a\-zA\-Z0\-9\_\\\-\]\+\)/?$

    Content-Type: application/json



<br>
<br>

# /api/event/import

    Content-Type: application/json

## POST


**Input Schema**
```json
{
    "additionalProperties": false,
    "properties": {
        "dataChannelId": {
            "type": "string"
        },
        "source": {
            "type": "string"
        }
    },
    "required": [
        "source",
        "dataChannelId"
    ],
    "type": "object"
}
```



**Output Schema**
```json
null
```



**Notes**

POST an event to be created from an existing data channel

* `source`: set to "capture"
* `dataChannelId`: id of the data channel to be imported



<br>
<br>

# /api/stories/\(?P\<story\_id\>\[a\-zA\-Z0\-9\_\\\-\]\+\)

    Content-Type: application/json


