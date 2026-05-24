#! /usr/bin/env python3

import sys
from optparse import OptionParser

import jmdaemon
from jmbase import commands, get_log
from jmbase.support import EXIT_ARGERROR, JM_CORE_VERSION
from jmclient import (
    JMClientProtocolFactory,
    JMMakerClientProtocol,
    Maker,
    add_base_options,
    get_mchannels,
    jm_single,
    load_program_config,
    start_reactor,
)
from twisted.python.log import startLogging

jlog = get_log()


class DNMakerClientProtocol(JMMakerClientProtocol):
    @commands.JMUp.responder
    def on_JM_UP(self):
        d = self.callRemote(
            commands.JMSetup,
            role="MAKER",
            initdata=self.client.offerlist,
            use_fidelity_bond=(self.client.fidelity_bond is not None),
        )
        self.defaultCallbacks(d)
        return {"accepted": True}


class DNJMClientProtocolFactory(JMClientProtocolFactory):
    def __init__(self, client, proto_type="TAKER"):
        self.client = client
        self.proto_client = None
        self.proto_type = proto_type
        if self.proto_type == "MAKER":
            self.protocol = DNMakerClientProtocol


def announce_no_orders(self, orderlist, nick, fidelity_bond_proof_msg, new_mc):
    return


jmdaemon.MessageChannelCollection.announce_orders = announce_no_orders


class DNMaker(Maker):
    def __init__(self):
        self.fidelity_bond = None
        self.offerlist = []
        self.aborted = False

    def create_my_orders(self):
        return []

    def oid_to_order(self, cjorder, amount):
        pass

    def on_tx_unconfirmed(self, cjorder, txid):
        pass

    def on_tx_confirmed(self, cjorder, txid, confirmations):
        pass

    def get_fidelity_bond_template(self):
        return None


def directory_node_startup():
    parser = OptionParser(usage="usage: %prog [options]")
    add_base_options(parser)
    (options, args) = parser.parse_args()
    options = vars(options)
    if len(args) != 1:
        parser.error(
            "One argument required: string to be published in the MOTD "
            "of the directory node."
        )
        sys.exit(EXIT_ARGERROR)

    operator_message = args[0]
    load_program_config(config_path=options["datadir"], bs="no-blockchain")
    mchan_config = get_mchannels()[0]
    node_location = mchan_config["directory_nodes"]
    jmdaemon.onionmc.server_handshake_json[
        "motd"
    ] = "DIRECTORY NODE: {}\nJOINMARKET VERSION: {}\n{}".format(
        node_location, JM_CORE_VERSION, operator_message
    )
    maker = DNMaker()
    jlog.info("starting directory node")
    clientfactory = DNJMClientProtocolFactory(maker, proto_type="MAKER")
    nodaemon = jm_single().config.getint("DAEMON", "no_daemon")
    daemon = bool(nodaemon)
    if jm_single().config.get("BLOCKCHAIN", "network") in [
        "regtest",
        "testnet",
        "signet",
    ]:
        startLogging(sys.stdout)
    start_reactor(
        jm_single().config.get("DAEMON", "daemon_host"),
        jm_single().config.getint("DAEMON", "daemon_port"),
        clientfactory,
        daemon=daemon,
    )


if __name__ == "__main__":
    directory_node_startup()
