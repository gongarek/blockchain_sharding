from beacon import Beacon
from validators import Validator
from notarries import Nottaries
from communicator import Communicator
from plot import plot_network
from plot import plot_transaction_shard
from time import time
import csv


class Main:
    def __init__(s):
        s.__sim_time = 3

    def get__sim_time(s):
        return s.__sim_time


if __name__ == "__main__":
    main = Main()
    communicator = Communicator()
    # import pydevd
    # port_mapping = [46877, 42985, 37251, 40053]
    # pydevd.settrace('localhost', port=port_mapping[communicator.rank], stdoutToServer=True, stderrToServer=True)

    if communicator.comm.rank == 0:
        beacon = Beacon()
        beacon.boot_beacon()
    communicator.comm.barrier()
    if communicator.comm.rank != 0:
        validators = Validator()
        notarries = Nottaries()
    if communicator.rank == 1:
        time_list = [0]
        transactions_nb = [0]
        gg = True
    for tick in range(main.get__sim_time()):
        if communicator.rank == 0:
            beacon.choose_rotated_notarries()
            if tick % 3 == 0:
                beacon.choose_rotated_validators()
        # communicator.comm.barrier()
        if communicator.rank != 0:
            notarries.shuffle_nodes(communicator.comm.recv(source=0, tag=3))
            if tick % 3 == 0:
                validators.shuffle_nodes(communicator.comm.recv(source=0, tag=4))
            validators.send_trans_to_beacon(list(validators.get__vali_peers_in_shard()), validators.get__all_val_ids())
        # communicator.comm.barrier()
        if communicator.rank == 0:
            trans_received = []
            for rank in range(1, communicator.nbRanks):
                trans_received.append(communicator.comm.recv(source=rank, tag=6))
            trans_after_del = beacon.tran_acc_balance(trans_received)
            beacon.resend_transaction(trans_after_del)
        # communicator.comm.barrier()
        if communicator.rank != 0:
            ramificat = validators.crate_ramification(validators.get__all_val_ids(), validators.shard_blockchain)
            correct_block = validators.check_block_time(ramificat)
            validators.approve_block(correct_block, validators.get__all_val_ids())
            validators.walidate_blockchain(correct_block)
            validators.hide_transactions()
            message = notarries.check_data_availability(validators.shard_blockchain[-1])
            verdict = notarries.walidate_challenge(message, validators.shard_blockchain[-1])
            if verdict:
                validators.recognized_hider(correct_block)
        # communicator.comm.barrier()
        if communicator.rank == 0:
            beacon.burn_stake_bad_commit_availability(8)
            beacon.burn_stake_notarry()
            beacon.burn_stake_bad_commit_availability(10)
            beacon.remove_indebted_validators()
            beacon.remove_indebted_notarries()
        # communicator.comm.barrier()
        if communicator.rank != 0:
            validators.change_validators_ids(communicator.comm.recv(source=0, tag=11))
            notarries.change_notarries_ids(communicator.comm.recv(source=0, tag=12))
            if communicator.rank == 1:
                if gg == True:
                    start_time = time()
                    gg = False
                time_list.append(time()-start_time)
                transactions_nb.append(tick*validators.get__trans_per_block()*communicator.nbRanks)
        # communicator.comm.barrier()
    if communicator.rank == 0:
        pass
        # plot_network(beacon.get__peers_in_beacon(), communicator.rank)
    else:
        # plot_network(validators.get__vali_peers_in_shard(), communicator.rank)
        if communicator.rank == 1:
            # with open('skalowalnosc.csv', mode='w') as skalowalnosc:
            #     employee_writer = csv.writer(skalowalnosc, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            #     for i in zip(time, transactions_nb):
            #         skalowalnosc = csv.writerow(list(i))
            plot_transaction_shard(time_list,transactions_nb)
