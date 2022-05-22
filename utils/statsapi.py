from statsapi import get
from statsapi import endpoints
from statsapi import meta
import logging
import requests


def full_schedule(
    date=None,
    start_date=None,
    end_date=None,
    team="",
    opponent="",
    sportId=1,
    game_id=None,
):
    """Get list of games for a given date/range and/or team/opponent."""
    if end_date and not start_date:
        date = end_date
        end_date = None

    if start_date and not end_date:
        date = start_date
        start_date = None

    params = {}

    if date:
        params.update({"date": date})
    elif start_date and end_date:
        params.update({"startDate": start_date, "endDate": end_date})

    if team != "":
        params.update({"teamId": str(team)})

    if opponent != "":
        params.update({"opponentId": str(opponent)})

    if game_id:
        params.update({"gamePks": game_id})

    params.update(
        {
            "sportId": str(sportId),
            "fields": "dates,date,games,gamePk,status,codedGameState,totalGames,totalGamesInProgress,seriesGameNumber",
        }
    )

    r = get("schedule", params)

    games = []
    if r.get("totalItems") == 0:
        return games
    else:
        for date in r.get("dates"):
            games.append(date)
        return games


BASE_URL = endpoints.BASE_URL
"""Base MLB Stats API URL"""
ENDPOINTS = endpoints.ENDPOINTS
"""MLB Stats API endpoint configuration"""

logger = logging.getLogger("statsapi")
# indentical to statsapi.get except prints URL, retries 524 error 5 times, force=True.
def get_big(endpoint, params, force=True):
    """Call MLB StatsAPI and return JSON data.

    This function is for advanced querying of the MLB StatsAPI,
    and is used by the functions in this library.
    """
    # Lookup endpoint from input parameter
    ep = ENDPOINTS.get(endpoint)
    if not ep:
        raise ValueError("Invalid endpoint (" + str(endpoint) + ").")

    url = ep["url"]
    logger.debug("URL: {}".format(url))

    path_params = {}
    query_params = {}

    # Parse parameters into path and query parameters, and discard invalid parameters
    for p, pv in params.items():
        if ep["path_params"].get(p):
            logger.debug("Found path param: {}".format(p))
            if ep["path_params"][p].get("type") == "bool":
                if str(pv).lower() == "false":
                    path_params.update({p: ep["path_params"][p].get("False", "")})
                elif str(pv).lower() == "true":
                    path_params.update({p: ep["path_params"][p].get("True", "")})
            else:
                path_params.update({p: str(pv)})
        elif p in ep["query_params"]:
            logger.debug("Found query param: {}".format(p))
            query_params.update({p: str(pv)})
        else:
            if force:
                logger.debug(
                    "Found invalid param, forcing into query parameters per force flag: {}".format(
                        p
                    )
                )
                query_params.update({p: str(pv)})
            else:
                logger.debug("Found invalid param, ignoring: {}".format(p))

    logger.debug("path_params: {}".format(path_params))
    logger.debug("query_params: {}".format(query_params))

    # Replace path parameters with their values
    for k, v in path_params.items():
        logger.debug("Replacing {%s}" % k)
        url = url.replace(
            "{" + k + "}",
            ("/" if ep["path_params"][k]["leading_slash"] else "")
            + v
            + ("/" if ep["path_params"][k]["trailing_slash"] else ""),
        )
        logger.debug("URL: {}".format(url))

    while url.find("{") != -1 and url.find("}") > url.find("{"):
        param = url[url.find("{") + 1 : url.find("}")]
        if ep.get("path_params", {}).get(param, {}).get("required"):
            if (
                ep["path_params"][param]["default"]
                and ep["path_params"][param]["default"] != ""
            ):
                logger.debug(
                    "Replacing {%s} with default: %s."
                    % (param, ep["path_params"][param]["default"])
                )
                url = url.replace(
                    "{" + param + "}", ep["path_params"][param]["default"]
                )
            else:
                if force:
                    logger.warning(
                        "Missing required path parameter {%s}, proceeding anyway per force flag..."
                        % param
                    )
                else:
                    raise ValueError("Missing required path parameter {%s}" % param)
        else:
            logger.debug("Removing optional param {%s}" % param)
            url = url.replace("{" + param + "}", "")

        logger.debug("URL: {}".format(url))
    # Add query parameters to the URL
    if len(query_params) > 0:
        for k, v in query_params.items():
            logger.debug("Adding query parameter {}={}".format(k, v))
            sep = "?" if url.find("?") == -1 else "&"
            url += sep + k + "=" + v
            logger.debug("URL: {}".format(url))

    # Make sure required parameters are present
    satisfied = False
    missing_params = []
    for x in ep.get("required_params", []):
        if len(x) == 0:
            satisfied = True
        else:
            missing_params.extend([a for a in x if a not in query_params])
            if len(missing_params) == 0:
                satisfied = True
                break

    if not satisfied and not force:
        if ep.get("note"):
            note = "\n--Endpoint note: " + ep.get("note")
        else:
            note = ""

        raise ValueError(
            "Missing required parameter(s): "
            + ", ".join(missing_params)
            + ".\n--Required parameters for the "
            + endpoint
            + " endpoint: "
            + str(ep.get("required_params", []))
            + ". \n--Note: If there are multiple sets in the required parameter list, you can choose any of the sets."
            + note
        )

    # Make the request
    r = requests.get(url)
    # print(url)
    flag = 0
    while r.status_code == 524:
        r = requests.get(url)
        # sleep(10)
        flag += 1
        print(flag)
        if flag > 4:
            break
    if r.status_code not in [200, 201]:
        r.raise_for_status()
    else:
        return r.json()

    return None


def event_types(bool_filt="", code_only=True):
    # bool_filt: hit, baseRunningEvent, or plateAppearance
    event_types = meta("eventTypes")
    if bool_filt:
        event_types = [x for x in event_types if x.get(bool_filt)]
    if code_only:
        event_types = [x["code"] for x in event_types]
    return event_types
