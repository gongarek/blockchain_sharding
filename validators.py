from communicator import Communicator
from beacon import Beacon
from transaction import Transaction
from block import Block
from plot import plot_network
from random import choice, random, sample
from copy import deepcopy
from time import time


class Validator:
    def __init__(s):
        s.communicator = Communicator()
        s.beacon = Beacon()
        s.__val_peers_in_shard = {}
        s.__validators_peers = 2
        s.__all_val_ids = s.communicator.comm.recv(source=0, tag=1)
        s.__transaction_per_block = 20
        s.__tran_max_pay = 300
        s.__max_stake = 4000
        s._shard_blockchain = [Block(None, None, time(), None, None)]   ##tu moze sie zmieni dla publicznych
        shard_vali_ids = []
        vali_per_rank = s.beacon.get__vali_per_rank()
        for i in range((s.communicator.rank - 1) * vali_per_rank, s.communicator.rank * vali_per_rank):
            shard_vali_ids.append(s.__all_val_ids[i])
        for node in shard_vali_ids:
            s.__val_peers_in_shard[node] = sample((set(shard_vali_ids)-{node}), s.__validators_peers)
        plot_network(s.__val_peers_in_shard, s.communicator.rank)
    
    """Gettery"""
    def get__vali_peers_in_shard(s):
        return s.__val_peers_in_shard

    def get__all_val_ids(s):
        return s.__all_val_ids

    def get__trans_per_block(s):
        return s.__transaction_per_block

    @property
    def shard_blockchain(s):
        return s._shard_blockchain


    """ROTACJA VALIDATORIAMI"""
    def shuffle_validators(s, migrant_ids):
        # usuwanie perrow poprzez rotowane wezly
        for m in migrant_ids:
            del s.__val_peers_in_shard[m]
        p_o_n = deepcopy(s.__val_peers_in_shard)
        for peer in p_o_n.items():
            indexes = []
            for index, val, in enumerate(peer[1]):
                if val in migrant_ids:
                    indexes.append(index)
            for i in indexes[::-1]:
                del s.__val_peers_in_shard[peer[0]][i]
        s.send_recv_migrants(migrant_ids)

    def send_recv_migrants(s, migrant_ids): # tu nie daje do wszystkich tagow, bo sie to zmienia
        if s.communicator.rank == 1:
            s.communicator.comm.send(migrant_ids, dest=2)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.nbRanks - 1, tag=5))
        elif s.communicator.rank == (s.communicator.nbRanks-1):
            s.communicator.comm.send(migrant_ids, dest=1, tag=5)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.rank-1))
        else:
            s.communicator.comm.send(migrant_ids, dest=s.communicator.rank+1)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.rank - 1))

    def supp_peers(s, recv_migrant_id):
        for i in recv_migrant_id:
            s.__val_peers_in_shard[i] = []
        keys = list(s.__val_peers_in_shard.keys())
        for u in keys:
            while len(s.__val_peers_in_shard[u]) < s.__validators_peers:
                s.__val_peers_in_shard[u].append(choice(list(set(keys) - {u} - set(s.__val_peers_in_shard[u]))))
                
    """TRANSAKCJE"""
    def send_trans_to_beacon(s, nodes_in_shard, node_ids):
        shard_transactions = []
        for i in range(s.__transaction_per_block):
            sender = choice(nodes_in_shard)
            receiver = choice(list((set(node_ids) - {sender})))
            amount = choice(range(1, s.__tran_max_pay))
            shard_transactions.append(Transaction(sender, receiver, amount))
        s.communicator.comm.send(shard_transactions, dest=0, tag=6)

    """ZLE UCZYNKI"""
    def crate_ramification(s, nodes_in_shard, blockchain):
        ramification = []
        finally_transactions = s.communicator.comm.recv(source=0, tag=7)  # TE TRANSAKCJE SA DOBRE.
        money_in_block = 0
        for tran in finally_transactions:
            money_in_block += tran.amount
        while True:
            staker = choice(nodes_in_shard)
            stake = choice(list(range(money_in_block, s.__max_stake + 1)))
            block = Block(finally_transactions, blockchain[-1].get__block_id(), time(), staker, stake)
            block.create_tree()
            ramification.append(block)
            if random() < 0.5:  # randomowo tworzone sa rozgalezienia, czyli nowe bloki.Dobre, sa zle czasy
                break
        return ramification

    def check_block_time(s, ramification):
        early = min([block.get__time() for block in ramification])
        return next(block for block in ramification if block.get__time() == early)
    
    def approve_block(s, correct_block, nodes_in_shard):
        hostility = random()
        if hostility < 0.5:
            s._shard_blockchain.append(correct_block)
        else:
            money_in_block = 0
            for tran in correct_block.get__transactions():
                money_in_block += tran.amount
            staker = choice(nodes_in_shard)
            stake = choice(list(range(money_in_block, s.__max_stake + 1)))
            block = Block(correct_block.get__transactions(), hash("whatever"), time(), staker, stake)
            block.create_tree()
            s._shard_blockchain.append(block)

    """AKA RYBAK COS ZROBIC Z 2/3"""
    def walidate_blockchain(s, correct_block): # Sprawdzany jest ostatni, bo i tak co ture sie sprawdza. I JEST ZAWSZE ich dwa. Albo dobry albo zly
        fraud = "None"
        if s._shard_blockchain[-1].get__parent() is not None: # niepotrzebne. zabezpieczenie przed pierwszym blokiem
            if s._shard_blockchain[-1].get__parent() != s._shard_blockchain[-2].get__block_id():
                fraud = [s._shard_blockchain[-1].get__staker(), s._shard_blockchain[-1].get__stake()]
                del s._shard_blockchain[-1]
                s._shard_blockchain.append(correct_block)  # dodawanie poprawnego
        s.communicator.comm.send(fraud, dest=0, tag=8)  # none nie mozna wysylac . dzieki temu bedziemy karac

    def hide_transactions(s): # usuwa czesc transakcji. Dokladnie jedna
        [index] = sample(range(len(s._shard_blockchain[-1].get__transactions())), k=1)
        del s._shard_blockchain[-1].get__transactions()[index]

    def recognized_hider(s, correct_block):
        del s._shard_blockchain[-1]
        s._shard_blockchain.append(correct_block)
        
    """receive ids to change"""
    def change_validators_ids(s, change_ids):  # W SUMIE TO SLABE BO ZAMIENIA WEZLAMI ,ALE TAK NAPRAWDE POWINNO BYC USUWANKO I POTEM DOBIOR WEZLOW TAK JAK NA GORZE ROBILEM, ALE JUZ NIE CHCE MI SIE
        new_val_peers_in_shard = deepcopy(s.__val_peers_in_shard)
        for key, val in new_val_peers_in_shard.items():
            for change in change_ids:
                for index, vali in enumerate(val):
                    if vali == change[0]:
                        s.__val_peers_in_shard[key][index] = change[1]
        for key in new_val_peers_in_shard:
            for change in change_ids:
                if key == change[0]:
                    s.__val_peers_in_shard[change[1]] = s.__val_peers_in_shard[change[0]]
        for change in change_ids:
            if change[0] in s.__val_peers_in_shard.keys():
                s.__val_peers_in_shard.pop(change[0])
        for change in change_ids:
            for index, node in enumerate(s.__all_val_ids):
                if node == change[0]:
                    s.__all_val_ids[index] = change[1]
