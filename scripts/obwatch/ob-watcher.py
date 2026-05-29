#!/usr/bin/env python3
from functools import cmp_to_key

import base64
import hashlib
import html
import http.server
import io
import json
import os
import threading
import time
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from optparse import OptionParser
from typing import Tuple, Union
from twisted.internet import reactor
from urllib.parse import parse_qs

from jmbase import bintohex
from jmbase.support import EXIT_FAILURE
from jmbitcoin import bitcoin_unit_to_power, sat_to_unit, sat_to_unit_power
from jmclient import FidelityBondMixin, get_interest_rate, check_and_start_tor
from jmclient.fidelity_bond import FidelityBondProof

import sybil_attack_calculations as sybil

from jmbase import get_log
log = get_log()

try:
    import matplotlib
except:
    log.warning("matplotlib not found, charts will not be available. "
                "Do `pip install matplotlib` in the joinmarket virtual environment.")

if 'matplotlib' in sys.modules:
    # https://stackoverflow.com/questions/2801882/generating-a-png-with-matplotlib-when-display-is-undefined
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

from jmclient import jm_single, load_program_config, calc_cj_fee, \
     get_mchannels, add_base_options
from jmdaemon import (OrderbookWatch, MessageChannelCollection,
                      OnionMessageChannel, IRCMessageChannel)
#TODO this is only for base58, find a solution for a client without jmbitcoin
import jmbitcoin as btc
from jmdaemon.protocol import *

bond_exponent = None

#Initial state: allow only SW offer types
sw0offers = list(filter(lambda x: x[0:3] == 'sw0', offername_list))
swoffers = list(filter(lambda x: x[0:3] == 'swa' or x[0:3] == 'swr', offername_list))
filtered_offername_list = sw0offers

rotateObform = '<form action="rotateOb" method="post"><input type="submit" value="Rotate orderbooks"/></form>'
refresh_orderbook_form = '<form action="refreshorderbook" method="post"><input type="submit" value="Refresh directory sources" /></form>'
sorted_units = ('BTC', 'mBTC', '&#956;BTC', 'satoshi')
sorted_rel_units = ('%', '&#8241;', 'ppm')
rel_unit_to_factor = {'%': 100, '&#8241;': 1e4, 'ppm': 1e6}


def calc_depth_data(db, value):
    pass


def get_graph_html(fig):
    imbuf = io.BytesIO()
    fig.savefig(imbuf, format='png')
    b64 = base64.b64encode(imbuf.getvalue()).decode('utf-8')
    return '<img src="data:image/png;base64,' + b64 + '" />'


# callback functions for displaying order data
def do_nothing(arg, order, btc_unit, rel_unit):
    return arg


def ordertype_display(ordertype, order, btc_unit, rel_unit):
    ordertypes = {'sw0absoffer': 'Native SW Absolute Fee', 'sw0reloffer': 'Native SW Relative Fee',
                  'swabsoffer': 'SW Absolute Fee', 'swreloffer': 'SW Relative Fee'}
    return ordertypes[ordertype]


def cjfee_display(cjfee: Union[Decimal, float, int],
                  order: dict,
                  btc_unit: str,
                  rel_unit: str) -> str:
    if order['ordertype'] in ['swabsoffer', 'sw0absoffer']:
        val = sat_to_unit(cjfee, html.unescape(btc_unit))
        if btc_unit == "BTC":
            return "%.8f" % val
        else:
            return str(val)
    elif order['ordertype'] in ['reloffer', 'swreloffer', 'sw0reloffer']:
        return str(Decimal(cjfee) * Decimal(rel_unit_to_factor[rel_unit])) + rel_unit


def order_str(s, order, btc_unit, rel_unit):
    return str(s)


def bond_value_to_str(bond_value: Decimal, btc_unit: str) -> str:
    if btc_unit == "BTC":
        return "%.16f" % bond_value
    elif btc_unit == "mBTC":
        return "%.10f" % bond_value
    else:
        return str(bond_value)


def create_offerbook_table_heading(btc_unit, rel_unit):
    col = '  <th>{1}</th>\n'  # .format(field,label)
    tableheading = '<table class="tftable sortable" border="1">\n <tr>' + ''.join(
            [
                col.format('ordertype', 'Type'),
                col.format('counterparty', 'Counterparty'),
                col.format('oid', 'Order ID'),
                col.format('cjfee', 'Fee'),
                col.format('txfee', 'Miner Fee Contribution / ' + btc_unit),
                col.format('minsize', 'Minimum Size / ' + btc_unit),
                col.format('maxsize', 'Maximum Size / ' + btc_unit),
                col.format('bondvalue', 'Bond value / ' + btc_unit + '<sup>' + bond_exponent + '</sup>')
            ]) + ' </tr>'
    return tableheading

def create_bonds_table_heading(btc_unit):
    tableheading = ('<table class="tftable sortable" border="1"><tr>'
        + '<th>Counterparty</th>'
        + '<th>UTXO</th>'
        + '<th>Bond value / ' + btc_unit + '<sup>' + bond_exponent + '</sup></th>'
        + '<th>Locktime</th>'
        + '<th>Locked coins / ' + btc_unit + '</th>'
        + '<th>Confirmation time</th>'
        + '<th>Signature expiry height</th>'
        + '<th>Redeem script</th>'
        + '</tr>'
    )
    return tableheading

def create_choose_units_form(selected_btc, selected_rel):
    choose_units_form = (
        '<form method="get" action="">' +
        '<select name="btcunit" onchange="this.form.submit();">' +
        ''.join(('<option>' + u + ' </option>' for u in sorted_units)) +
        '</select><select name="relunit" onchange="this.form.submit();">' +
        ''.join(('<option>' + u + ' </option>' for u in sorted_rel_units)) +
        '</select></form>')
    choose_units_form = choose_units_form.replace(
            '<option>' + selected_btc,
            '<option selected="selected">' + selected_btc)
    choose_units_form = choose_units_form.replace(
            '<option>' + selected_rel,
            '<option selected="selected">' + selected_rel)
    return choose_units_form

def get_fidelity_bond_data(taker):
    started = time.monotonic()
    fbonds = taker.get_visible_fidelitybond_rows()
    blocks = jm_single().bc_interface.get_current_block_height()
    mediantime = jm_single().bc_interface.get_best_block_median_time()
    interest_rate = get_interest_rate()
    cache_key = (blocks, mediantime, interest_rate,
                 tuple((fb["counterparty"], fb["takernick"], fb["proof"])
                       for fb in fbonds))
    cache = getattr(taker, "_fidelity_bond_data_cache", None)
    if cache and cache.get("key") == cache_key:
        return cache["value"]

    bond_utxo_set = set()
    fidelity_bond_data = []
    bond_outpoint_conf_times = []
    fidelity_bond_values = []
    for fb in fbonds:
        try:
            parsed_bond = FidelityBondProof.parse_and_verify_proof_msg(fb["counterparty"],
                fb["takernick"], fb["proof"])
        except ValueError:
            continue
        bond_utxo_data = FidelityBondMixin.get_validated_timelocked_fidelity_bond_utxo(
            parsed_bond.utxo, parsed_bond.utxo_pub, parsed_bond.locktime, parsed_bond.cert_expiry,
            blocks)
        if bond_utxo_data == None:
            continue
        #check for duplicated utxos i.e. two or more makers using the same UTXO
        # which is obviously not allowed, a fidelity bond must only be usable by one maker nick
        utxo_str = parsed_bond.utxo[0] + b":" + str(parsed_bond.utxo[1]).encode("ascii")
        if utxo_str in bond_utxo_set:
            continue
        bond_utxo_set.add(utxo_str)

        fidelity_bond_data.append((parsed_bond, bond_utxo_data))
        conf_time = jm_single().bc_interface.get_block_time(
            jm_single().bc_interface.get_block_hash(
                blocks - bond_utxo_data["confirms"] + 1
            )
        )
        bond_outpoint_conf_times.append(conf_time)

        bond_value = FidelityBondMixin.calculate_timelocked_fidelity_bond_value(
            bond_utxo_data["value"],
            conf_time,
            parsed_bond.locktime,
            mediantime,
            interest_rate)
        fidelity_bond_values.append(bond_value)
    result = (fidelity_bond_data, fidelity_bond_values,
              bond_outpoint_conf_times)
    taker._fidelity_bond_data_cache = {"key": cache_key, "value": result}
    elapsed = time.monotonic() - started
    if elapsed >= 0.25:
        log.info("Computed fidelity-bond export data in {:.3f}s "
                 "(visible_bonds={}, valid_bonds={})".format(
                     elapsed, len(fbonds), len(fidelity_bond_data)))
    return result

class OrderbookPageRequestHeader(http.server.SimpleHTTPRequestHandler):
    def __init__(self, request, client_address, base_server):
        self.taker = base_server.taker
        self.base_server = base_server
        http.server.SimpleHTTPRequestHandler.__init__(
                self, request, client_address, base_server,
                directory=os.path.dirname(os.path.realpath(__file__)))

    def create_orderbook_obj(self):
        rows = self.taker.get_visible_orderbook_rows()
        fbonds = self.taker.get_visible_fidelitybond_rows()

        fidelitybonds = []
        if fbonds and jm_single().bc_interface != None:
            (fidelity_bond_data, fidelity_bond_values, bond_outpoint_conf_times) =\
                get_fidelity_bond_data(self.taker)
            fidelity_bond_values_dict = dict([(bond_data.maker_nick, bond_value)
                for (bond_data, _), bond_value in zip(fidelity_bond_data, fidelity_bond_values)])
            for ((parsed_bond, bond_utxo_data), fidelity_bond_value, bond_outpoint_conf_time)\
                    in zip(fidelity_bond_data, fidelity_bond_values, bond_outpoint_conf_times):
                fb = {
                    "counterparty": parsed_bond.maker_nick,
                    "utxo": {"txid": bintohex(parsed_bond.utxo[0]),
                        "vout": parsed_bond.utxo[1]},
                    "bond_value": fidelity_bond_value,
                    "locktime": parsed_bond.locktime,
                    "amount":  bond_utxo_data["value"],
                    "script": bintohex(bond_utxo_data["script"]),
                    "utxo_confirmations": bond_utxo_data["confirms"],
                    "utxo_confirmation_timestamp": bond_outpoint_conf_time,
                    "utxo_pub": bintohex(parsed_bond.utxo_pub),
                    "cert_expiry": parsed_bond.cert_expiry
                }
                fidelitybonds.append(fb)
        else:
            fidelity_bond_values_dict = {}

        offers = []
        for row in rows:
            o = dict(row)
            if 'cjfee' in o:
                if o['ordertype'] == 'swabsoffer'\
                   or o['ordertype'] == 'sw0absoffer':
                    o['cjfee'] = int(o['cjfee'])
                else:
                    o['cjfee'] = str(Decimal(o['cjfee']))
            o["fidelity_bond_value"] = fidelity_bond_values_dict.get(o["counterparty"], 0)
            offers.append(o)

        return {"offers": offers, "fidelitybonds": fidelitybonds}

    def create_orderbook_sources_obj(self):
        return {
            "directory_peers": [dict(row)
                                for row in self.taker.get_directory_peer_rows()],
            "orderbook_sources": [dict(row)
                                  for row in
                                  self.taker.get_orderbook_source_rows()],
            "fidelitybond_sources": [
                dict(row)
                for row in self.taker.get_fidelitybond_source_rows()],
        }

    def create_depth_chart(self, cj_amount, args=None):
        if 'matplotlib' not in sys.modules:
            return 'matplotlib not installed, charts not available'

        if args is None:
            args = {}
        rows = self.taker.get_visible_orderbook_rows()
        sqlorders = [o for o in rows if o["ordertype"] in filtered_offername_list]
        orderfees = sorted([calc_cj_fee(o['ordertype'], o['cjfee'], cj_amount) / 1e8
                            for o in sqlorders
                            if o['minsize'] <= cj_amount <= o[
                                'maxsize']])

        if len(orderfees) == 0:
            return 'No orders at amount ' + str(cj_amount / 1e8)
        fig = plt.figure()
        scale = args.get("scale")
        if (scale is not None) and (scale[0] == "log"):
            orderfees = [float(fee) for fee in orderfees]
            if orderfees[0] > 0:
                ratio = orderfees[-1] / orderfees[0]
                step = ratio ** 0.0333  # 1/30
                bins = [orderfees[0] * (step ** i) for i in range(30)]
            else:
                ratio = orderfees[-1] / 1e-8  # single satoshi placeholder
                step = ratio ** 0.0333  # 1/30
                bins = [1e-8 * (step ** i) for i in range(30)]
                bins[0] = orderfees[0]  # replace placeholder
            plt.xscale('log')
        else:
            bins = 30
        if len(orderfees) == 1:  # these days we have liquidity, but just in case...
            plt.hist(orderfees, bins, rwidth=0.8, range=(0, orderfees[0] * 2))
        else:
            plt.hist(orderfees, bins, rwidth=0.8)
        plt.grid()
        plt.title('CoinJoin Orderbook Depth Chart for amount=' + str(cj_amount /
                                                                     1e8) + 'btc')
        plt.xlabel('CoinJoin Fee / btc')
        plt.ylabel('Frequency')
        return get_graph_html(fig)

    def create_size_histogram(self, args):
        if 'matplotlib' not in sys.modules:
            return 'matplotlib not installed, charts not available'

        rows = self.taker.get_visible_orderbook_rows()
        rows = [o for o in rows if o["ordertype"] in filtered_offername_list]
        ordersizes = sorted([r['maxsize'] / 1e8 for r in rows])

        fig = plt.figure()
        scale = args.get("scale")
        if (scale is not None) and (scale[0] == "log"):
            ratio = ordersizes[-1] / ordersizes[0]
            step = ratio ** 0.0333  # 1/30
            bins = [ordersizes[0] * (step ** i) for i in range(30)]
        else:
            bins = 30
        plt.hist(ordersizes, bins, histtype='bar', rwidth=0.8)
        if bins != 30:
            fig.axes[0].set_xscale('log')
        plt.grid()
        plt.xlabel('Order sizes / btc')
        plt.ylabel('Frequency')
        return get_graph_html(fig) + ("<br/><a href='?scale=log'>log scale</a>" if
                                      bins == 30 else "<br/><a href='?'>linear</a>")

    def create_fidelity_bond_table(self, btc_unit: str) -> Tuple[str, str]:
        if jm_single().bc_interface == None:
            fbonds = self.taker.get_visible_fidelitybond_rows()
            fidelity_bond_data = []
            for fb in fbonds:
                try:
                    proof = FidelityBondProof.parse_and_verify_proof_msg(
                        fb["counterparty"],
                        fb["takernick"],
                        fb["proof"])
                except ValueError:
                    proof = None
                fidelity_bond_data.append((proof, None))
            fidelity_bond_values = [-1]*len(fidelity_bond_data) #-1 means no data
            bond_outpoint_conf_times = [-1]*len(fidelity_bond_data)
            total_btc_committed_str = "unknown"
        else:
            (fidelity_bond_data, fidelity_bond_values, bond_outpoint_conf_times) =\
                get_fidelity_bond_data(self.taker)
            total_btc_committed_str = str(sat_to_unit(
                sum([utxo_data["value"] for _, utxo_data in fidelity_bond_data]),
                html.unescape(btc_unit)))

        RETARGET_INTERVAL = 2016
        elem = lambda e: f"<td>{e}</td>"
        bondtable = ""
        for (bond_data, utxo_data), bond_value, conf_time in zip(
                fidelity_bond_data, fidelity_bond_values, bond_outpoint_conf_times):

            if bond_value == -1 or conf_time == -1 or utxo_data == None:
                bond_value_str = "No data"
                conf_time_str = "No data"
                utxo_value_str = "No data"
            else:
                bond_value_str = bond_value_to_str(sat_to_unit_power(bond_value,
                    2 * bitcoin_unit_to_power(html.unescape(btc_unit))),
                    html.unescape(btc_unit))
                conf_time_str = str(datetime.utcfromtimestamp(0) + timedelta(seconds=conf_time))
                utxo_value_str = sat_to_unit(utxo_data["value"], html.unescape(btc_unit))
            bondtable += ("<tr>"
                + elem(bond_data.maker_nick)
                + elem(bintohex(bond_data.utxo[0]) + ":" + str(bond_data.utxo[1]))
                + elem(bond_value_str)
                + elem((datetime.utcfromtimestamp(0) + timedelta(seconds=bond_data.locktime)).strftime("%Y-%m-%d"))
                + elem(utxo_value_str)
                + elem(conf_time_str)
                + elem(str(bond_data.cert_expiry*RETARGET_INTERVAL))
                + elem(bintohex(btc.mk_freeze_script(bond_data.utxo_pub,
                    bond_data.locktime)))
                + "</tr>"
            )

        heading2 = (str(len(fidelity_bond_data)) + " fidelity bonds found with "
            + total_btc_committed_str + " " + btc_unit
            + " total locked up")
        choose_units_form = (
            '<form method="get" action="">' +
            '<select name="btcunit" onchange="this.form.submit();">' +
            ''.join(('<option>' + u + ' </option>' for u in sorted_units)) +
            '</select></form>')
        choose_units_form = choose_units_form.replace(
                '<option>' + btc_unit,
                '<option selected="selected">' + btc_unit)

        decodescript_tip = ("<br/>Tip: try running the RPC <code>decodescript "
            + "&lt;redeemscript&gt;</code> as proof that the fidelity bond address matches the "
            + "locktime.<br/>Also run <code>gettxout &lt;utxo_txid&gt; &lt;utxo_vout&gt;</code> "
            + "as proof that the fidelity bond UTXO is real.")

        return (heading2,
            choose_units_form + create_bonds_table_heading(btc_unit) + bondtable + "</table>"
            + decodescript_tip)

    def create_sybil_resistance_page(self, btc_unit: str) -> Tuple[str, str]:
        if jm_single().bc_interface == None:
            return "", "Calculations unavailable, requires configured bitcoin node."

        (fidelity_bond_data, fidelity_bond_values, bond_outpoint_conf_times) =\
            get_fidelity_bond_data(self.taker)

        choose_units_form = (
            '<form method="get" action="">' +
            '<select name="btcunit" onchange="this.form.submit();">' +
            ''.join(('<option>' + u + ' </option>' for u in sorted_units)) +
            '</select></form>')
        choose_units_form = choose_units_form.replace(
                '<option>' + btc_unit,
                '<option selected="selected">' + btc_unit)
        mainbody = choose_units_form

        honest_weight = sum(fidelity_bond_values)
        mainbody += ("Assuming the makers in the offerbook right now are not sybil attackers, "
            + "how much would a sybil attacker starting now have to sacrifice to succeed in their"
            + " attack with 95% probability. Honest weight="
            + str(sat_to_unit_power(honest_weight, 2 * bitcoin_unit_to_power(html.unescape(btc_unit)))) + " " + btc_unit
            + "<sup>" + bond_exponent + "</sup><br/>Also assumes that takers "
            + "are not price-sensitive and that their max "
            + "coinjoin fee is configured high enough that they dont exclude any makers.")
        heading2 = "Sybil attacks from external enemies."

        mainbody += ('<table class="tftable" border="1"><tr>'
            + '<th>Maker count</th>'
            + '<th>6month locked coins / ' + btc_unit + '</th>'
            + '<th>1y locked coins / ' + btc_unit + '</th>'
            + '<th>2y locked coins / ' + btc_unit + '</th>'
            + '<th>5y locked coins / ' + btc_unit + '</th>'
            + '<th>10y locked coins / ' + btc_unit + '</th>'
            + '<th>Required burned coins / ' + btc_unit + '</th>'
            + '</tr>'
        )

        timelocks = [0.5, 1.0, 2.0, 5.0, 10.0, None]
        interest_rate = get_interest_rate()
        for makercount, unit_success_sybil_weight in sybil.successful_attack_95pc_sybil_weight.items():
            success_sybil_weight = unit_success_sybil_weight * honest_weight
            row = "<tr><td>" + str(makercount) + "</td>"
            for timelock in timelocks:
                if timelock != None:
                    coins_per_sybil = sybil.weight_to_locked_coins(success_sybil_weight,
                        interest_rate, timelock)
                else:
                    coins_per_sybil = sybil.weight_to_burned_coins(success_sybil_weight)
                row += ("<td>" + str(sat_to_unit(coins_per_sybil * makercount, html.unescape(btc_unit)))
                    + "</td>")
            row += "</tr>"
            mainbody += row
        mainbody += "</table>"

        mainbody += ("<h2>Sybil attacks from enemies within</h2>Assume a sybil attack is ongoing"
            + " right now and that the counterparties with the most valuable fidelity bonds are "
            + " actually controlled by the same entity. Then, what is the probability of a "
            + " successful sybil attack for a given makercount, and what is the fidelity bond "
            + " value being foregone by not putting all bitcoins into just one maker.")
        mainbody += ('<table class="tftable" border="1"><tr>'
            + '<th>Maker count</th>'
            + '<th>Success probability</th>'
            + '<th>Foregone value / ' + btc_unit + '<sup>' + bond_exponent + '</sup></th>'
            + '</tr>'
        )

        #limited because calculation is slow, so this avoids server being too slow to respond
        MAX_MAKER_COUNT_INTERNAL = 10
        weights = sorted(fidelity_bond_values)[::-1]
        for makercount in range(1, MAX_MAKER_COUNT_INTERNAL+1):
            makercount_str = (str(makercount) + " - " + str(MAX_MAKER_COUNT_INTERNAL)
                if makercount == len(fidelity_bond_data) and len(fidelity_bond_data) !=
                MAX_MAKER_COUNT_INTERNAL else str(makercount))
            success_prob = sybil.calculate_top_makers_sybil_attack_success_probability(weights,
                makercount)
            total_sybil_weight = sum(weights[:makercount])
            sacrificed_values = [sybil.weight_to_burned_coins(w) for w in weights[:makercount]]
            foregone_value = (sybil.coins_burned_to_weight(sum(sacrificed_values))
                - total_sybil_weight)
            mainbody += ("<tr><td>" + makercount_str + "</td><td>" + str(round(success_prob * 100.0, 5))
                + "%</td><td>" + bond_value_to_str(sat_to_unit_power(
                    foregone_value, 2 * bitcoin_unit_to_power(
                        html.unescape(btc_unit))), html.unescape(btc_unit))
                + "</td></tr>")
            if makercount == len(weights):
                break
        mainbody += "</table>"

        return heading2, mainbody

    def create_orderbook_table(self, btc_unit: str, rel_unit: str) -> Tuple[int, str]:
        result = ''
        rows = self.taker.get_visible_orderbook_rows()
        if not rows:
            return 0, result
        rows = [o for o in rows if o["ordertype"] in filtered_offername_list]

        if jm_single().bc_interface == None:
            for row in rows:
                row["bondvalue"] = "No data"
        else:
            blocks = jm_single().bc_interface.get_current_block_height()
            mediantime = jm_single().bc_interface.get_best_block_median_time()
            interest_rate = get_interest_rate()
            fbonds_by_counterparty = dict([
                (fbond["counterparty"], fbond)
                for fbond in self.taker.get_visible_fidelitybond_rows()])
            for row in rows:
                fbond_data = fbonds_by_counterparty.get(row["counterparty"])
                if fbond_data is None:
                    row["bondvalue"] = "0"
                    continue
                else:
                    try:
                        parsed_bond = FidelityBondProof.parse_and_verify_proof_msg(
                            fbond_data["counterparty"],
                            fbond_data["takernick"],
                            fbond_data["proof"]
                        )
                    except ValueError:
                        row["bondvalue"] = "0"
                        continue
                    utxo_data = FidelityBondMixin.get_validated_timelocked_fidelity_bond_utxo(
                        parsed_bond.utxo, parsed_bond.utxo_pub, parsed_bond.locktime,
                        parsed_bond.cert_expiry, blocks)
                    if utxo_data == None:
                        row["bondvalue"] = "0"
                        continue
                    bond_value = FidelityBondMixin.calculate_timelocked_fidelity_bond_value(
                        utxo_data["value"],
                        jm_single().bc_interface.get_block_time(
                            jm_single().bc_interface.get_block_hash(
                                blocks - utxo_data["confirms"] + 1
                            )
                        ),
                        parsed_bond.locktime,
                        mediantime,
                        interest_rate)
                    row["bondvalue"] = bond_value_to_str(sat_to_unit_power(
                        bond_value,
                        2 * bitcoin_unit_to_power(html.unescape(btc_unit))),
                        html.unescape(btc_unit))

        def _okd_satoshi_to_unit(sat, order, btc_unit, rel_unit):
            val = sat_to_unit(sat, html.unescape(btc_unit))
            if btc_unit == "BTC":
                return "%.8f" % val
            else:
                return str(val)

        order_keys_display = (('ordertype', ordertype_display),
                              ('counterparty', do_nothing),
                              ('oid', order_str),
                              ('cjfee', cjfee_display),
                              ('txfee', _okd_satoshi_to_unit),
                              ('minsize', _okd_satoshi_to_unit),
                              ('maxsize', _okd_satoshi_to_unit),
                              ('bondvalue', do_nothing))

        def _cmp(x, y):
            if x < y:
                return -1
            elif x > y:
                return 1
            else:
                return 0

        # somewhat complex sorting to sort by cjfee but with swabsoffers on top
        def orderby_cmp(x, y):
            if x['ordertype'] == y['ordertype']:
                return _cmp(Decimal(x['cjfee']), Decimal(y['cjfee']))
            return _cmp(offername_list.index(x['ordertype']),
                       offername_list.index(y['ordertype']))

        for o in sorted(rows, key=cmp_to_key(orderby_cmp)):
            result += ' <tr>\n'
            for key, displayer in order_keys_display:
                result += '  <td>' + str(displayer(o[key], o, btc_unit,
                                               rel_unit)) + '</td>\n'
            result += ' </tr>\n'
        return len(rows), result

    def get_counterparty_count(self):
        rows = self.taker.get_visible_orderbook_rows()
        counterparties = set(
            [row["counterparty"] for row in rows
             if row["ordertype"] in filtered_offername_list])
        return str(len(counterparties))

    def do_GET(self):
        # http.server.SimpleHTTPRequestHandler.do_GET(self)
        # print('httpd received ' + self.path + ' request')
        self.path, query = self.path.split('?', 1) if '?' in self.path else (
            self.path, '')
        args = parse_qs(query)
        pages = ['/', '/fidelitybonds', '/ordersize', '/depth',
            '/sybilresistance', '/orderbook.json', '/orderbook-sources.json']
        static_files = {'/vendor/sorttable.js', '/vendor/bootstrap.min.css', '/vendor/jquery-3.5.1.slim.min.js'}
        if self.path in static_files or self.path not in pages:
            return super().do_GET()
        fd = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
            'orderbook.html'), 'r')
        orderbook_fmt = fd.read()
        fd.close()
        alert_msg = ''
        if jm_single().joinmarket_alert[0]:
            alert_msg = '<br />JoinMarket Alert Message:<br />' + \
                        jm_single().joinmarket_alert[0]
        if self.path == '/':
            btc_unit = args['btcunit'][
                0] if 'btcunit' in args else sorted_units[0]
            rel_unit = args['relunit'][
                0] if 'relunit' in args else sorted_rel_units[0]
            if btc_unit not in sorted_units:
                btc_unit = sorted_units[0]
            if rel_unit not in sorted_rel_units:
                rel_unit = sorted_rel_units[0]
            ordercount, ordertable = self.create_orderbook_table(
                    btc_unit, rel_unit)
            choose_units_form = create_choose_units_form(btc_unit, rel_unit)
            table_heading = create_offerbook_table_heading(btc_unit, rel_unit)
            replacements = {
                'PAGETITLE': 'JoinMarket Browser Interface',
                'MAINHEADING': 'JoinMarket Orderbook',
                'SECONDHEADING':
                    (str(ordercount) + ' orders found by ' +
                     self.get_counterparty_count() + ' counterparties' + alert_msg),
                'MAINBODY': (
                    rotateObform + refresh_orderbook_form + choose_units_form +
                    table_heading + ordertable + '</table>\n')
            }
        elif self.path == '/fidelitybonds':
            btc_unit = args['btcunit'][0] if 'btcunit' in args else sorted_units[0]
            if btc_unit not in sorted_units:
                btc_unit = sorted_units[0]
            heading2, mainbody = self.create_fidelity_bond_table(btc_unit)

            replacements = {
                'PAGETITLE': 'JoinMarket Browser Interface',
                'MAINHEADING': 'Fidelity Bonds',
                'SECONDHEADING': heading2,
                'MAINBODY': mainbody
            }
        elif self.path == '/ordersize':
            replacements = {
                'PAGETITLE': 'JoinMarket Browser Interface',
                'MAINHEADING': 'Order Sizes',
                'SECONDHEADING': 'Order Size Histogram' + alert_msg,
                'MAINBODY': self.create_size_histogram(args)
            }
        elif self.path.startswith('/depth'):
            # if self.path[6] == '?':
            #	quantity =
            cj_amounts = [10 ** cja for cja in range(4, 12, 1)]
            mainbody = [self.create_depth_chart(cja, args) \
                        for cja in cj_amounts] + \
                       ["<br/><a href='?'>linear</a>" if args.get("scale") \
                            else "<br/><a href='?scale=log'>log scale</a>"]
            replacements = {
                'PAGETITLE': 'JoinMarket Browser Interface',
                'MAINHEADING': 'Depth Chart',
                'SECONDHEADING': 'Orderbook Depth' + alert_msg,
                'MAINBODY': '<br />'.join(mainbody)
            }
        elif self.path == '/sybilresistance':
            btc_unit = args['btcunit'][0] if 'btcunit' in args else sorted_units[0]
            if btc_unit not in sorted_units:
                btc_unit = sorted_units[0]
            heading2, mainbody = self.create_sybil_resistance_page(btc_unit)
            replacements = {
                'PAGETITLE': 'JoinMarket Browser Interface',
                'MAINHEADING': 'Resistance to Sybil Attacks from Fidelity Bonds',
                'SECONDHEADING': heading2,
                'MAINBODY': mainbody
            }
        elif self.path == '/orderbook.json':
            replacements = {}
            orderbook_fmt = json.dumps(self.create_orderbook_obj())
        elif self.path == '/orderbook-sources.json':
            replacements = {}
            orderbook_fmt = json.dumps(self.create_orderbook_sources_obj())
        orderbook_page = orderbook_fmt
        for key, rep in replacements.items():
            orderbook_page = orderbook_page.replace(key, rep)
        self.send_response(200)
        if self.path.endswith('.json'):
            self.send_header('Content-Type', 'application/json')
        else:
            self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', len(orderbook_page))
        self.end_headers()
        self.wfile.write(orderbook_page.encode('utf-8'))

    def get_url_base(self) -> str:
        # This is to handle the case where the server is behind a reverse proxy
        # and base path may not be /.
        # First we get HTTP or HTTPS protocol from Origin header and then use
        # Host header to get the base path.
        # Will work with nginx config like this:
        # location /ob-watcher {
        #     rewrite /ob-watcher/(.*) /$1 break;
        #     proxy_pass http://localhost:62601;
        #     proxy_set_header Host $host/ob-watcher;
        # }
        is_https = self.headers.get('Origin', '').startswith('https://')
        host = self.headers.get('Host', '')
        return 'https://' + host if is_https else 'http://' + host

    def do_POST(self):
        global filtered_offername_list
        pages = ['/refreshorderbook', '/rotateOb']
        if self.path not in pages:
            return
        if self.path == '/refreshorderbook':
            self.taker.refresh_orderbook_from_connected_directories(
                'manual HTTP refresh')
            self.send_response(302)
            self.send_header('Location', self.get_url_base() + '/')
            self.end_headers()
        elif self.path == '/rotateOb':
            if filtered_offername_list == sw0offers:
                log.debug('Showing nested segwit orderbook')
                filtered_offername_list = swoffers
            elif filtered_offername_list == swoffers:
                log.debug('Showing native segwit orderbook')
                filtered_offername_list = sw0offers
            self.send_response(302)
            self.send_header('Location', self.get_url_base() + '/')
            self.end_headers()

class HTTPDThread(threading.Thread):
    def __init__(self, taker, hostport):
        threading.Thread.__init__(self, name='HTTPDThread')
        self.daemon = True
        self.taker = taker
        self.hostport = hostport

    def run(self):
        # hostport = ('localhost', 62601)
        try:
            httpd = http.server.HTTPServer(self.hostport,
                                          OrderbookPageRequestHeader)
        except Exception as e:
            print("Failed to start HTTP server: " + str(e))
            os._exit(EXIT_FAILURE)
        httpd.taker = self.taker
        print('\nstarted http server, visit http://{0}:{1}/\n'.format(
                *self.hostport))
        httpd.serve_forever()


class ObBasic(OrderbookWatch):
    """Dummy orderbook watch class
    with hooks for triggering orderbook request"""
    def __init__(self, msgchan, hostport):
        self.hostport = hostport
        self.httpd_thread_started = False
        self.orderbook_refresh_thread_started = False
        self.orderbook_refresh_interval = 900
        try:
            self.orderbook_refresh_interval = int(os.environ.get(
                'JM_OBWATCH_REFRESH_INTERVAL_SECONDS',
                str(self.orderbook_refresh_interval)))
        except ValueError:
            log.warning('Invalid JM_OBWATCH_REFRESH_INTERVAL_SECONDS; '
                        'using 900 seconds.')
        self.reconnect_refresh_min_interval = 30
        try:
            self.reconnect_refresh_min_interval = int(os.environ.get(
                'JM_OBWATCH_RECONNECT_REFRESH_MIN_INTERVAL_SECONDS',
                str(self.reconnect_refresh_min_interval)))
        except ValueError:
            log.warning('Invalid '
                        'JM_OBWATCH_RECONNECT_REFRESH_MIN_INTERVAL_SECONDS; '
                        'using 30 seconds.')
        self.last_reconnect_refresh_at = {}
        self.directory_source_stale_seconds = 900
        try:
            self.directory_source_stale_seconds = int(os.environ.get(
                'JM_OBWATCH_DN_SOURCE_STALE_SECONDS',
                str(self.directory_source_stale_seconds)))
        except ValueError:
            log.warning('Invalid JM_OBWATCH_DN_SOURCE_STALE_SECONDS; '
                        'using 900 seconds.')
        self.directory_refresh_response_timeout_seconds = 30
        try:
            self.directory_refresh_response_timeout_seconds = int(
                os.environ.get(
                    'JM_OBWATCH_DN_REFRESH_RESPONSE_TIMEOUT_SECONDS',
                    str(self.directory_refresh_response_timeout_seconds)))
        except ValueError:
            log.warning('Invalid '
                        'JM_OBWATCH_DN_REFRESH_RESPONSE_TIMEOUT_SECONDS; '
                        'using 30 seconds.')
        self.source_inactive_grace_seconds = 900
        try:
            self.source_inactive_grace_seconds = int(os.environ.get(
                'JM_OBWATCH_SOURCE_INACTIVE_GRACE_SECONDS',
                str(self.source_inactive_grace_seconds)))
        except ValueError:
            log.warning('Invalid JM_OBWATCH_SOURCE_INACTIVE_GRACE_SECONDS; '
                        'using 900 seconds.')
        self.orphan_source_retention_seconds = 3600
        try:
            self.orphan_source_retention_seconds = int(os.environ.get(
                'JM_OBWATCH_ORPHAN_SOURCE_RETENTION_SECONDS',
                str(self.orphan_source_retention_seconds)))
        except ValueError:
            log.warning('Invalid '
                        'JM_OBWATCH_ORPHAN_SOURCE_RETENTION_SECONDS; '
                        'using 3600 seconds.')
        self.refresh_run_lock = threading.Lock()
        self.refresh_state_lock = threading.Lock()
        self.refresh_tracking = False
        self.refresh_expected_directory_peers = set()
        self.refresh_disconnected_directory_peers = set()
        self.refresh_seen_directory_peers = set()
        self.pending_refresh_directories = set()
        self.pending_reconnect_refresh = False
        self.set_msgchan(msgchan)
        # in client-server, this is passed by client
        # in INIT message. Here, we have no Joinmarket client,
        # but we have access to the client config in this script:
        self.dust_threshold = jm_single().DUST_THRESHOLD
        self.register_directory_connected_hook()
        # Start HTTP endpoint even if welcome callback never arrives.
        self.start_http_server_once()
        self.start_orderbook_refresh_once()

    def start_http_server_once(self):
        if self.httpd_thread_started:
            return
        HTTPDThread(self, self.hostport).start()
        self.httpd_thread_started = True

    def on_order_seen(self, counterparty, oid, ordertype, minsize, maxsize,
                      txfee, cjfee, source_directory=None):
        with self.refresh_state_lock:
            if self.refresh_tracking and source_directory:
                self.refresh_seen_directory_peers.add(source_directory)
        super().on_order_seen(counterparty, oid, ordertype, minsize, maxsize,
                              txfee, cjfee, source_directory)

    def on_fidelity_bond_seen(self, nick, bond_type,
                              fidelity_bond_proof_msg, source_directory=None):
        with self.refresh_state_lock:
            if self.refresh_tracking and source_directory:
                self.refresh_seen_directory_peers.add(source_directory)
        super().on_fidelity_bond_seen(nick, bond_type,
                                      fidelity_bond_proof_msg,
                                      source_directory)

    def on_message_seen(self, mc, msgtype, nick, message,
                        source_directory=None):
        with self.refresh_state_lock:
            if self.refresh_tracking and source_directory and \
                    source_directory in self.refresh_expected_directory_peers:
                self.refresh_seen_directory_peers.add(source_directory)
        super().on_message_seen(mc, msgtype, nick, message,
                                source_directory)

    def on_directory_peer_disconnected(self, peer):
        try:
            peer_location = peer.peer_location()
        except Exception:
            peer_location = 'unknown'
        if peer_location != 'unknown':
            self.record_directory_disconnected(peer_location)
        with self.refresh_state_lock:
            if self.refresh_tracking and \
                    peer_location in self.refresh_expected_directory_peers:
                self.refresh_disconnected_directory_peers.add(peer_location)
        log.info('Directory peer {} disconnected during orderbook '
                 'tracking.'.format(peer_location))

    def on_disconnect(self):
        offers, fbonds = self.get_orderbook_counts()
        self.pending_reconnect_refresh = True
        log.warning('Message channel disconnected; preserving current '
                    'orderbook view ({} offers, {} fidelity bonds) '
                    'until a directory reconnect refresh succeeds.'.format(
                        offers, fbonds))

    def get_orderbook_counts(self):
        return (len(self.get_visible_orderbook_rows()),
                len(self.get_visible_fidelitybond_rows()))

    def get_connected_directory_locations(self):
        channels = getattr(self.msgchan, 'mchannels', [self.msgchan])
        connected_locations = set()
        for mc in channels:
            if hasattr(mc, 'get_connected_directory_peers'):
                try:
                    for peer in mc.get_connected_directory_peers():
                        connected_locations.add(peer.peer_location())
                except Exception as e:
                    log.debug('Could not inspect connected directory peers: ' +
                              repr(e))
        return connected_locations

    def begin_refresh_tracking(self, expected_directory_peers):
        with self.refresh_state_lock:
            self.refresh_tracking = True
            self.refresh_expected_directory_peers = set(
                expected_directory_peers)
            self.refresh_disconnected_directory_peers = set()
            self.refresh_seen_directory_peers = set()

    def get_refresh_tracking_state(self):
        with self.refresh_state_lock:
            return {
                'directory_peers': set(self.refresh_seen_directory_peers),
                'disconnected_directory_peers':
                    set(self.refresh_disconnected_directory_peers),
            }

    def end_refresh_tracking(self):
        with self.refresh_state_lock:
            self.refresh_tracking = False
            self.refresh_expected_directory_peers = set()
            self.refresh_disconnected_directory_peers = set()
            self.refresh_seen_directory_peers = set()

    def make_refresh_id(self, reason):
        normalized_reason = reason.replace(' ', '-')
        return '{}-{}'.format(normalized_reason[:40], uuid.uuid4().hex)

    def get_directory_refresh_metadata(self):
        with self.dblock:
            rows = self.db.execute('SELECT * FROM directory_peers;').fetchall()
        return {row['directory']: dict(row) for row in rows}

    def get_stale_directory_locations(self, candidate_directories=None):
        connected_directories = self.get_connected_directory_locations()
        if candidate_directories is not None:
            connected_directories &= set(candidate_directories)
        if self.directory_source_stale_seconds <= 0:
            return connected_directories
        now = int(time.time())
        metadata = self.get_directory_refresh_metadata()
        stale_directories = set()
        for directory in connected_directories:
            row = metadata.get(directory, {})
            last_response = row.get('last_orderbook_response_at') or \
                row.get('last_seen_at') or row.get('last_successful_refresh_at')
            if last_response is None or \
                    now - int(last_response) >= \
                    self.directory_source_stale_seconds:
                stale_directories.add(directory)
        return stale_directories

    def selective_refresh_orderbook(self, reason, candidate_directories=None,
                                    force=False):
        if not self.refresh_run_lock.acquire(False):
            log.info('Skipping {} orderbook refresh; another refresh is '
                     'already in progress.'.format(reason))
            if candidate_directories:
                with self.refresh_state_lock:
                    self.pending_refresh_directories.update(
                        candidate_directories)
                self.pending_reconnect_refresh = True
            return False
        before_offers, before_fbonds = self.get_orderbook_counts()
        refresh_id = self.make_refresh_id(reason)
        pending_directories = set()
        try:
            connected_directories = self.get_connected_directory_locations()
            if candidate_directories is not None:
                connected_directories &= set(candidate_directories)
            if not connected_directories:
                self.pending_reconnect_refresh = True
                log.warning('Skipping {} orderbook refresh; no connected '
                            'candidate directory peers are available.'.format(
                                reason))
                return False
            target_directories = connected_directories if force else \
                self.get_stale_directory_locations(candidate_directories)
            if not target_directories:
                log.info('Skipping {} orderbook refresh; all candidate '
                         'directory sources are fresh.'.format(reason))
                return True
            self.begin_refresh_tracking(target_directories)
            self.set_current_refresh_id(refresh_id)
            log.info('Starting {} selective orderbook refresh for {} '
                     'directory peers with current view of {} offers, {} '
                     'fidelity bonds.'.format(
                         reason, len(target_directories), before_offers,
                         before_fbonds))
            sent_directories = set()
            for directory in sorted(target_directories):
                self.record_orderbook_request(directory, refresh_id)
                try:
                    if self.request_orderbook_from_directory(directory):
                        sent_directories.add(directory)
                    else:
                        log.warning('Failed to send targeted orderbook '
                                    'request to directory {}.'.format(
                                        directory))
                except Exception as e:
                    log.warning('Failed to send targeted orderbook request '
                                'to directory {}: {}'.format(
                                    directory, repr(e)))
            if not sent_directories:
                self.pending_reconnect_refresh = True
                return False

            time.sleep(max(
                0, self.directory_refresh_response_timeout_seconds))
            tracking_state = self.get_refresh_tracking_state()
            disconnected_directories = \
                tracking_state['disconnected_directory_peers']
            disconnected_directories |= (
                sent_directories - self.get_connected_directory_locations())
            responded_directories = (
                tracking_state['directory_peers'] & sent_directories)

            if disconnected_directories:
                log.warning('{} orderbook refresh saw directory disconnects: '
                            '{}.'.format(
                                reason,
                                ','.join(sorted(disconnected_directories))))

            if responded_directories:
                self.prune_unseen_sources_for_directories(
                    responded_directories, refresh_id)
                for directory in responded_directories:
                    self.record_successful_refresh(directory, refresh_id)

            missing_directories = sent_directories - responded_directories
            self.pending_reconnect_refresh = bool(missing_directories)
            self.prune_sources()
            after_offers, after_fbonds = self.get_orderbook_counts()
            log.info('Completed {} selective orderbook refresh: {} '
                     'directories responded, {} missing; current view '
                     'has {} offers, {} fidelity bonds.'.format(
                         reason, len(responded_directories),
                         len(missing_directories), after_offers,
                         after_fbonds))
            return not missing_directories
        finally:
            self.set_current_refresh_id(None)
            self.end_refresh_tracking()
            self.refresh_run_lock.release()
            with self.refresh_state_lock:
                pending_directories = set(self.pending_refresh_directories)
                self.pending_refresh_directories = set()
            if pending_directories:
                self.start_reconnect_orderbook_refresh(
                    'queued directory peer refresh', pending_directories)

    def refresh_orderbook_from_connected_directories(self, reason):
        return self.selective_refresh_orderbook(
            reason, self.get_connected_directory_locations(), force=True)

    def start_reconnect_orderbook_refresh(self, reason, target_directories):
        threading.Thread(target=self.selective_refresh_orderbook,
                         args=(reason, target_directories, True),
                         name='ReconnectOrderbookRefreshThread',
                         daemon=True).start()

    def register_directory_connected_hook(self):
        channels = getattr(self.msgchan, 'mchannels', [self.msgchan])
        for mc in channels:
            if hasattr(mc, 'on_directory_peer_connected'):
                mc.on_directory_peer_connected = \
                    self.on_directory_peer_connected
            if hasattr(mc, 'on_directory_peer_disconnected'):
                mc.on_directory_peer_disconnected = \
                    self.on_directory_peer_disconnected

    def on_directory_peer_connected(self, peer):
        if self.reconnect_refresh_min_interval < 0:
            return
        try:
            peer_location = peer.peer_location()
        except Exception:
            peer_location = 'unknown'
        if peer_location != 'unknown':
            self.record_directory_connected(peer_location)
        now = time.monotonic()
        if self.reconnect_refresh_min_interval and \
                peer_location != 'unknown' and \
                now - self.last_reconnect_refresh_at.get(
                    peer_location, 0) < \
                self.reconnect_refresh_min_interval:
            log.info('Skipping directory reconnect orderbook refresh for {}; '
                     'previous refresh was recent.'.format(peer_location))
            return
        if peer_location != 'unknown':
            self.last_reconnect_refresh_at[peer_location] = now
        log.info('Directory peer {} connected; requesting targeted '
                 'orderbook refresh.'.format(peer_location))
        self.start_reconnect_orderbook_refresh(
            'directory peer {} connected'.format(peer_location),
            {peer_location} if peer_location != 'unknown' else None)

    def start_orderbook_refresh_once(self):
        if self.orderbook_refresh_thread_started:
            return
        if self.orderbook_refresh_interval <= 0:
            log.info('Periodic orderbook refresh disabled.')
            return
        threading.Thread(target=self.periodic_orderbook_refresh,
                         name='OrderbookRefreshThread',
                         daemon=True).start()
        self.orderbook_refresh_thread_started = True
        log.info('Periodic orderbook refresh every {} seconds.'.format(
            self.orderbook_refresh_interval))

    def periodic_orderbook_refresh(self):
        while True:
            time.sleep(self.orderbook_refresh_interval)
            try:
                log.info('Requesting periodic selective orderbook refresh.')
                self.selective_refresh_orderbook('periodic')
            except Exception as e:
                log.warning('Periodic orderbook refresh failed: ' + repr(e))

    def on_welcome(self):
        """TODO: It will probably be a bit
        simpler, and more consistent, to use
        a twisted http server here instead
        of a thread."""
        self.start_http_server_once()
        self.start_reconnect_orderbook_refresh(
            'startup', self.get_connected_directory_locations())

    def request_orderbook(self):
        self.msgchan.request_orderbook()

    def request_orderbook_from_directory(self, directory_location):
        if hasattr(self.msgchan, 'request_orderbook_from_directory'):
            return self.msgchan.request_orderbook_from_directory(
                directory_location)
        return False


"""An override for MessageChannel classes,
to allow receipt of privmsgs without the
verification hooks in client-daemon communication."""
def on_privmsg(inst, nick, message, source_directory=None):
    if len(message) < 2:
        return

    if message[0] != COMMAND_PREFIX:
        log.debug('message not a cmd')
        return
    cmd_string = message[1:].split(' ')[0]
    if cmd_string not in offername_list + fidelity_bond_cmd_list:
        log.debug('non-offer ignored')
        return
    #Ignore sigs (TODO better to include check)
    sig = message[1:].split(' ')[-2:]
    #reconstruct original message without cmd pref
    rawmessage = ' '.join(message[1:].split(' ')[:-2])
    for command in rawmessage.split(COMMAND_PREFIX):
        _chunks = command.split(" ")
        try:
            inst.check_for_orders(nick, _chunks, source_directory)
            inst.check_for_fidelity_bond(nick, _chunks, source_directory)
        except:
            pass

def get_dummy_nick():
    """In Joinmarket-CS nick creation is negotiated
    between client and server/daemon so as to allow
    client to sign for messages; here we only ever publish
    an orderbook request, so no such need, but for better
    privacy, a conformant nick is created based on a random
    pseudo-pubkey."""
    nick_pkh_raw = hashlib.sha256(os.urandom(10)).digest()[:NICK_HASH_LENGTH]
    nick_pkh = btc.base58.encode(nick_pkh_raw)
    #right pad to maximum possible; b58 is not fixed length.
    #Use 'O' as one of the 4 not included chars in base58.
    nick_pkh += 'O' * (NICK_MAX_ENCODED - len(nick_pkh))
    #The constructed length will be 1 + 1 + NICK_MAX_ENCODED
    nick = JOINMARKET_NICK_HEADER + str(JM_VERSION) + nick_pkh
    jm_single().nickname = nick
    return nick

def main():
    global bond_exponent
    parser = OptionParser(
            usage='usage: %prog [options]',
            description='Runs a webservice which shows the orderbook.')
    add_base_options(parser)
    parser.add_option('-H',
                      '--host',
                      action='store',
                      type='string',
                      dest='host',
                      default='localhost',
                      help='hostname or IP to bind to, default=localhost')
    parser.add_option('-p',
                      '--port',
                      action='store',
                      type='int',
                      dest='port',
                      help='port to listen on, default=62601',
                      default=62601)
    (options, args) = parser.parse_args()
    load_program_config(config_path=options.datadir)
    # needed to display notional units of FB valuation
    bond_exponent = jm_single().config.get("POLICY", "bond_value_exponent")
    try:
        float(bond_exponent)
    except ValueError:
        log.error("Invalid entry for bond_value_exponent, should be decimal "
                  "number: {}".format(bond_exponent))
        sys.exit(EXIT_FAILURE)
    check_and_start_tor()
    hostport = (options.host, options.port)
    mcs = []
    chan_configs = get_mchannels(mode="PASSIVE")
    for c in chan_configs:
        if "type" in c and c["type"] == "onion":
            mcs.append(OnionMessageChannel(c))
        else:
            # default is IRC; TODO allow others
            mcs.append(IRCMessageChannel(c))
    IRCMessageChannel.on_privmsg = on_privmsg
    OnionMessageChannel.on_privmsg = on_privmsg
    mcc = MessageChannelCollection(mcs)
    mcc.set_nick(get_dummy_nick())
    taker = ObBasic(mcc, hostport)
    log.info("Starting ob-watcher")
    mcc.run()



if __name__ == "__main__":
    main()
    reactor.run()
    print('done')
