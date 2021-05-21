"""
"""

import os
import calendar
import datetime
import operator
import flask
import pytz
from cassandra import cluster
from gevent import pywsgi

def route(pattern):
    """Drop-in replacement for method-friendly Flask-style routing
    """
    def decorated(method):
        method.pattern = pattern
        return method
    return decorated

class Server(flask.Flask):
    """
    """

    def __init__(self, clusterAddress=os.getenv("CLUSTER_ADDRESS", "54.67.105.220")):
        """
        """
        super().__init__(__name__)
        self.assignFlaskRoutesFromMethods()
        self.eastern = pytz.timezone("US/Eastern")
        self.clusterTwitterSeries = cluster.Cluster([clusterAddress])
        self.sessionTwitterSeries = self.clusterTwitterSeries.connect("twitterseries")
        self.clusterTopTrendingStreaming = cluster.Cluster([clusterAddress])
        self.sessionTopTrendingStreaming = self.clusterTopTrendingStreaming.connect("twittertrendingstreaming")
        self.clusterTopTrending = cluster.Cluster([clusterAddress])
        self.sessionTopTrending = self.clusterTopTrendingStreaming.connect("twittertrending")
        self.clusterStockData = cluster.Cluster([clusterAddress])
        self.sessionStockData = self.clusterStockData.connect("stockdata")
        self.clusterTweets = cluster.Cluster([clusterAddress])
        self.sessionTweets = self.clusterTweets.connect("latesttweets")

    def assignFlaskRoutesFromMethods(self):
        """Maps methods decorated with a "pattern" property to URL rules
        """
        for name in dir(self):
            if hasattr(self, name):
                attr = getattr(self, name)
                if hasattr(attr, "pattern"):
                    args = (attr.pattern, name, attr)
                    #print(args)
                    self.add_url_rule(*args)

    @route("/")
    def index(self):
        """
        """
        flask.url_for("static", filename="jquery.datetimepicker.css")
        flask.url_for("static", filename="jquery.datetimepicker.js")
        return flask.render_template("main.html")

    @route("/live_streaming")
    def live_streaming(self):
        """
        """
        data = []
        rows = self.sessionTopTrendingStreaming.execute("select * from toptrending30min where id in (0,1,2,3,4) order by timestamp desc limit 5")
        freq = []
        ticker = []
        color1 = []
        for r in rows:
            if r.sentiment > 0:
                color = "green"
            elif r.sentiment < 0:
                color = "red"
            else:
                color = "blue"
            temp = [r.ticker, r.frequency, color, r.ticker]
            freq.append(r.frequency)
            ticker.append(r.ticker)
            color1.append(color)
            data.append(temp)
        index, _ = zip(*sorted(enumerate(freq), key=operator.itemgetter(1)))
        data2 = []
        for i in index:
            temp = [ticker[i], freq[i], color1[i], ticker[i]]
            data2.append(temp)
        print(data2)
        text = []
        rowsTweets = self.sessionTweets.execute("select * from recenttweets limit 10")
        for r1 in rowsTweets:
            # print r1.tweet
            text.append(r1.tweet)
        # print rows.text
        author = [r.user for r in rowsTweets]
        dateTime = [str(r.year)+"-"+str(r.month)+"-"+str(r.day)+" " + str(r.hour)+":"+str(r.minute)+":"+str(r.second) for r in rowsTweets]
        return flask.jsonify(data=data2, text=text, author=author, dateTime=dateTime)

    @route("/live_streaming_tweets")
    def live_streaming_tweets(self):
        """
        """
        text = []
        rowsTweets = self.sessionTweets.execute("select * from recenttweets limit 10")
        for r1 in rowsTweets:
            # print r1.tweet
            text.append(r1.tweet)
        return flask.jsonify(data=text)

    @route("/top_trending_hour/<datetime1>")
    def top_trending_hour(self, datetime1):
        """
        """
        date = datetime.datetime.strptime(datetime1, "%Y_%m_%d_%H")
        date_eastern = self.eastern.localize(date, is_dst=None)
        date_utc = date_eastern.astimezone(pytz.utc)
        datetime2 = date_utc.strftime("%Y_%m_%d_%H")
        dt = datetime2.split("_")
        print(datetime1, dt)
        st = "select * from toptrendinghour where year="+str(int(dt[0]))+" and month="+str(int(dt[1]))+" and day=" + str(int(dt[2]))+" and hour="+str(int(dt[3]))+" limit 10"
        print(st)
        data = []
        rows = self.sessionTopTrending.execute(st)
        # "select * from toptrendinghour where year=2015 and month=1 and day=24 and hour=22 limit 10")
        for r in rows:
            if r.sentiment > 0:
                color = "green"
            elif r.sentiment < 0:
                color = "red"
            else:
                color = "blue"
            temp = [r.ticker, r.frequency, color, r.ticker]
            data.append(temp)
        print(data)
        return flask.jsonify(data=data)

    @route("/get_count_chart/<stockName>")
    def get_count_chart(self, stockName):
        """
        """
        rowTime = self.sessionTwitterSeries.execute("select * from trendingminute where ticker= "" + stockName + "" ")
        data1 = []
        data2 = []
        for r in rowTime:
            a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day, r.hour, r.minute).timetuple())*1000, r.frequency]
            ss = 0
            if r.sentiment > 0:
                ss = 1
            else:
                if r.sentiment < 0:
                    ss = -1
            b = [calendar.timegm(datetime.datetime(r.year, r.month, r.day, r.hour, r.minute).timetuple())*1000, ss]
            data1.append(a)
            data2.append(b)
        data1.reverse()
        data2.reverse()
        text = "Number of tweets/sentiment for "+stockName
        return flask.jsonify(data1=data1, data2=data2, text=text)

    @route("/get_correlation_chart/<stockName>")
    def get_correlation_chart(self, stockName):
        """
        """
        rows = self.sessionStockData.execute("select * from minutestock where ticker= "" + stockName + "" ")
        data = []
        print(rows)
        for r in rows:
            #a = [time.mktime(datetime.datetime(r.year, r.month, r.day).timetuple())*1000, r.open, r.high,  r.low, r.close]
            timestamp1 = pytz.timezone("US/Pacific").localize(datetime.datetime(r.year, r.month, r.day, r.hour, r.minute))
            #a = [time.mktime(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.open, r.high,  r.low, r.close]
            a = [calendar.timegm(timestamp1.astimezone(pytz.timezone("UTC")).timetuple())*1000, r.open, r.high,  r.low, r.close]
            data.append(a)
        data.reverse()
        rowTime = self.sessionTwitterSeries.execute("select * from trendingminute where ticker=  "" + stockName + "" ")
        data1 = []
        for r in rowTime:
            #a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day).timetuple())*1000, r.frequency]
            a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day, r.hour, r.minute).timetuple())*1000, r.frequency]
            data1.append(a)
        data1.reverse()
        text = "Stock Price / Mentions for "+stockName
        return flask.jsonify(data1=data, data2=data1, text=text)

def main():
    """
    """
    pywsgi.WSGIServer(("0.0.0.0", 80), Server()).serve_forever()

if __name__ == "__main__":
    main()
