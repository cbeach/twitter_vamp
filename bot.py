#!/usr/bin/env python

"""Bot class and main method.

This module contains the Bot class that is clubot and the main method to
connect and join irc channels, then stick around for commands.
"""

from twisted.internet import reactor
from client import CluBotFactory, CluBotContextFactory


class Bot:
    """Bot instance for clubot

    This defines the instance and connection parameters for clubot, and
    also contains a method to connect via ssl and start the reactor.
    """

    def __init__(self):
        """clubot info for connecting and joining channels"""
        self.host = 'irc.cat.pdx.edu'
        self.port = 6697
        self.chans = ['#movies', '#botgrounds']
        self.nick = 'clubot'

    def start(self):
        """connects over ssl to the irc server and starts the reactor"""
        reactor.connectSSL(self.host, self.port,
                           CluBotFactory(self.chans, self.nick),
                           CluBotContextFactory())
        reactor.run()


def main():
    """initializes clubot and starts the connection to irc"""

    clubot = Bot()
    clubot.start()

if __name__ == "__main__":
    main()
