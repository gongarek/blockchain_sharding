from communicator import Communicator
from beacon import Beacon
from block import Block
from random import choice, random, sample
from copy import deepcopy
from time import time


class Nottaries:
    def __init__(s):
        s.communicator = Communicator()
        s.beacon = Beacon()
        s.__notarries_peers_in_shard = {}
        s.__notarries_peers = 2
        s.__all_notarries_ids = s.communicator.comm.recv(source=0, tag=2)
        s.__availability_stake = 400
        shard_nottaries_ids = []
        notarry_per_rank = s.beacon.get__notarry_per_rank()
        for i in range((s.communicator.rank - 1) * notarry_per_rank, s.communicator.rank * notarry_per_rank):
            shard_nottaries_ids.append(s.__all_notarries_ids[i])
        for node in shard_nottaries_ids:
            s.__notarries_peers_in_shard[node] = sample((set(shard_nottaries_ids) - {node}), s.__notarries_peers)

    """ROTACJA WEZLAMI"""
    def shuffle_notarries(s, migrant_ids):
        # usuwanie perrow poprzez rotowane wezly
        for m in migrant_ids:
            del s.__notarries_peers_in_shard[m]
        p_o_n = deepcopy(s.__notarries_peers_in_shard)
        for peer in p_o_n.items():
            indexes = []
            for index, val, in enumerate(peer[1]):
                if val in migrant_ids:
                    indexes.append(index)
            for i in indexes[::-1]:
                del s.__notarries_peers_in_shard[peer[0]][i]
        s.send_recv_migrants(migrant_ids)

    def send_recv_migrants(s, migrant_ids):  # tu nie daje do wszystkich tagow, bo sie to zmienia
        if s.communicator.rank == 1:
            s.communicator.comm.send(migrant_ids, dest=2)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.nbRanks - 1, tag=5))
        elif s.communicator.rank == (s.communicator.nbRanks - 1):
            s.communicator.comm.send(migrant_ids, dest=1, tag=5)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.rank - 1))
        else:
            s.communicator.comm.send(migrant_ids, dest=s.communicator.rank + 1)
            s.supp_peers(s.communicator.comm.recv(source=s.communicator.rank - 1))

    def supp_peers(s, recv_migrant_id):
        for i in recv_migrant_id:
            s.__notarries_peers_in_shard[i] = []
        keys = list(s.__notarries_peers_in_shard.keys())
        for u in keys:
            while len(s.__notarries_peers_in_shard[u]) < s.__notarries_peers:
                s.__notarries_peers_in_shard[u].append(choice(list(set(keys) - {u} - set(s.__notarries_peers_in_shard[u]))))

    """DOSTEPNOSC DANYCH"""
    def check_data_availability(s, block_to_checked): # tu bedzie sprawdzana transakcja
        test_block = Block(block_to_checked.get__transactions(), None, time(), None, None)
        test_block.create_tree()
        test_nb_leaves = test_block.get__mt().get_leaf_count()
        number_of_leaves = block_to_checked.get__mt().get_leaf_count()
        staker = choice(list(s.__notarries_peers_in_shard))
        message = {'notar_staker' : staker}
        message['notar_stake'] = s.__availability_stake
        hostility = random()
        if hostility < 0.5:
            if test_nb_leaves != number_of_leaves:
                message['verdict'] = 'incomplete'
            else:
                message['verdict'] = 'complete'
        else:
            if test_nb_leaves != number_of_leaves:
                message['verdict'] = 'complete'
            else:
                message['verdict'] = 'incomplete'
        return message

    def walidate_challenge(s, message, block_to_checked):
        froud = 'None'
        test_block = Block(block_to_checked.get__transactions(), None, time(), None, None)
        test_block.create_tree()
        test_nb_leaves = test_block.get__mt().get_leaf_count()
        number_of_leaves = block_to_checked.get__mt().get_leaf_count()
        if test_nb_leaves != number_of_leaves:
            if message['verdict'] == 'incomplete':
                s.communicator.comm.send(froud, dest=0, tag=9)
                s.communicator.comm.send([block_to_checked.get__staker(), block_to_checked.get__stake()], dest=0, tag=10)
                return True
            else:
                froud = [message['notar_staker'], message['notar_stake']]
                s.communicator.comm.send(froud, dest=0, tag=9)
                s.communicator.comm.send([block_to_checked.get__staker(), block_to_checked.get__stake()], dest=0, tag=10)
                return True
        else:
            if message['verdict'] == 'complete':
                s.communicator.comm.send(froud, dest=0, tag=9)
                s.communicator.comm.send('None', dest=0, tag=10)
                return False
            else:
                froud = [message['notar_staker'], message['notar_stake']]
                s.communicator.comm.send(froud, dest=0, tag=9)
                s.communicator.comm.send('None', dest=0, tag=10)
                return False
    
    """receive ids to change"""
    def change_notarries_ids(s, change_ids):  # W SUMIE TO SLABE BO ZAMIENIA WEZLAMI ,ALE TAK NAPRAWDE POWINNO BYC USUWANKO I POTEM DOBIOR WEZLOW TAK JAK NA GORZE ROBILEM, ALE JUZ NIE CHCE MI SIE
        new_val_peers_in_shard = deepcopy(s.__notarries_peers_in_shard)
        for key, val in new_val_peers_in_shard.items():
            for change in change_ids:
                for index, vali in enumerate(val):
                    if vali == change[0]:
                        s.__notarries_peers_in_shard[key][index] = change[1]
        for key in new_val_peers_in_shard:
            for change in change_ids:
                if key == change[0]:
                    s.__notarries_peers_in_shard[change[1]] = s.__notarries_peers_in_shard[change[0]]
        for change in change_ids:
            if change[0] in s.__notarries_peers_in_shard.keys():
                s.__notarries_peers_in_shard.pop(change[0])
        for change in change_ids:
            for index, node in enumerate(s.__all_notarries_ids):
                if node == change[0]:
                    s.__all_notarries_ids[index] = change[1]
