import yaml
import math


input_files = [
'../testcase/testcase.txt' ,
'../input/updates.20150611.0845-0945.txt' ,
'../input/updates.20150612.0845-0945.txt' ,
'../input/updates.20150613.0845-0945.txt'
]

output_files = [
'../output/testcase.yaml',
'../output/updates.20150611.0845-0945.yaml',
'../output/updates.20150613.0845-0945.yaml',
'../output/updates.20150612.0845-0945.yaml'
]


def task1u3(updates,data):
    data[1] = len(updates)
    # total announcements received
    data[2] = sum(update['method'] == 'A' for update in updates)
    # total withdrawals received
    data[3] = sum(update['method'] == 'W' for update in updates)

    return data

def task4u6(preflist,data):
    data[4] = len(preflist)

    for prefix in preflist:

        has_a = False
        has_w = False

        for i in range(len(preflist[prefix]['updates'])):
            update = preflist[prefix]['updates'][i]
            if update['method'] == 'A':
                has_a = True
            elif update['method'] == 'W':
                has_w = True
        
        if has_a:
            data[5] += 1
        if has_w:
            data[6] += 1
    
    return data

def task7(preflist, data):
    for pref in preflist:
        if 'bursts' in preflist[pref]:
            data[7] += len(preflist[pref]['bursts'])
    
    return data

def task8(preflist, data):
    max_bursts = 0
    for pref in preflist:
        if 'bursts' in preflist[pref]:
            max_bursts = max(max_bursts, len(preflist[pref]['bursts']))
    data[8] = max_bursts

    return data

def task9(preflist, data):
    longest_burst = 0
    for pref in preflist:
        if 'bursts' in preflist[pref]:
            for burst in preflist[pref]['bursts']:
                longest_burst = max(longest_burst, burst['end']['timestamp'] - burst['start']['timestamp'])
    data[9] = longest_burst

    return data

def task10(preflist, data):
    # store longest burst for each prefix
    longest_burst_list = []
    for pref in preflist:
        # iterate over bursts and find longest burst
        longest_burst = 0
        if 'bursts' in preflist[pref]:
            for burst in preflist[pref]['bursts']:
                longest_burst = max(longest_burst, burst['end']['timestamp'] - burst['start']['timestamp'])
        longest_burst_list.append(longest_burst)
    # find average of longest bursts
    data[10] = math.ceil(sum(longest_burst_list)/len(preflist))

    return data

def task11(preflist, data):
    for pref in preflist:
        if not 'bursts' in preflist[pref]:
            data[11] += 1
    return data

def task12u14(preflist, data):
    for pref in preflist:
        if 'average_burst' in preflist[pref]:
            if preflist[pref]['average_burst'] > 600:
                data[12] += 1
            if preflist[pref]['average_burst'] > 1200:
                data[13] += 1 
            if preflist[pref]['average_burst'] > 1800:
                data[14] += 1
    return data

def calc_average_burst(preflist):
    for pref in preflist:
        average_burst = 0
        if 'bursts' in preflist[pref]:
            for burst in preflist[pref]['bursts']:
                average_burst += burst['end']['timestamp'] - burst['start']['timestamp']
            average_burst = average_burst/len(preflist[pref]['bursts'])
        preflist[pref]['average_burst'] = average_burst
    
    return preflist

def find_bursts(preflist):
    for prefix in preflist:
        
        i = 0

        while i < (len(preflist[prefix]['updates'])-1):
            update1 = preflist[prefix]['updates'][i]
            update2 = preflist[prefix]['updates'][i+1]

            # check for bursts
            if update2['timestamp'] - update1['timestamp'] < 240:
                # if prefix does not have bursts, add it
                if 'bursts' not in preflist[prefix]:
                    preflist[prefix]['bursts'] = []
                # set start end end update
                preflist[prefix]['bursts'].append({
                    'start': update1,
                    'end': update2
                }
                )
                while i < len(preflist[prefix]['updates'])-1 and preflist[prefix]['updates'][i+1]['timestamp'] - preflist[prefix]['updates'][i]['timestamp'] < 240:
                    i += 1
                # set end update of burst
                preflist[prefix]['bursts'][-1]['end'] = preflist[prefix]['updates'][i]
            
            # increment i
            i += 1
    return preflist

def build_list(filename):
    updates = []
    with open(filename) as file:
        for line in file:
            l = line.split('|')
            updates.append(
                {
                    'timestamp': int(l[1]),
                    'method' : l[2],
                    'prefix' : l[5].split("\n")[0],
                }
            )
    return updates

def build_pref_dict(updates):
    
    bypref = {}

    for update in updates:
        # if prefix not in bypref, add it
        if update['prefix'] not in bypref:
            bypref[update['prefix']] = {
                'updates': []
            }
        # add update to prefix
        bypref[update['prefix']]['updates'].append(
            {   
                'timestamp': update['timestamp'],
                'method' : update['method'],
            }
        )   
    return bypref   

def build_data_structures(filename):
    # build list of updates from file
    updates = build_list(filename)
    # build dictionary of prefixes
    by_prefix_dict = build_pref_dict(updates)
     # find bursts for each prefix
    by_prefix_dict = find_bursts(by_prefix_dict)
    # calculate average burst for each prefix
    by_prefix_dict = calc_average_burst(by_prefix_dict)

    return updates, by_prefix_dict

# run Tasks 1-14
def run_tasks(updates, bypref):
    # initialize data dictionary
    data = {
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 0,
        10: 0,
        11: 0,
        12: 0,
        13: 0,
        14: 0
    }

    data = task1u3(updates, data)
    data = task4u6(bypref, data)
    data = task7(bypref, data)
    data = task8(bypref, data)
    data = task9(bypref, data)
    data = task10(bypref, data)
    data = task11(bypref, data)
    data = task12u14(bypref, data)

    return data

if __name__ == "__main__":

    for i in range(len(input_files)):
        
        # build data structures
        updates, by_prefix_dict = build_data_structures(input_files[i])

        # run tasks using data structures
        data = run_tasks(updates, by_prefix_dict)


        # write data to outut file
        of = open(output_files[i],"w")
        of.write(yaml.dump(data))
        of.close()