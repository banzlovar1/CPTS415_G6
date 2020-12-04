import pandas as pd
import os
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from math import sin, cos, sqrt, atan2, radians
import time
from tqdm import tqdm

os.chdir("M:\\CPTS-415-Final")
df = pd.read_csv('.\\PythonData\\routes.csv')
af = pd.read_csv('.\\PythonData\\NewAirports.csv')
airlines = pd.read_csv('.\\PythonData\\airlines.csv')
testNodesT = pd.read_csv('.\\PythonData\\NewTestNodes.csv')
testNodesM = pd.read_csv('.\\PythonData\\testNodes1Mil.csv')
testNodesL = pd.read_csv('.\\PythonData\\testNodes5Mil.csv')


# g = nx.MultiGraph()
# # Generate graph of nodes using networkX
# for index, row in df.iterrows():
#     if row[3] != row[5]:
#         g.add_edge(row[3], row[5], weight=row[10])
# print(g.number_of_nodes())
# print(g.number_of_edges())

def getAllAirportsInCountry(airports, ct):
    return airports.loc[airports['Country'] == ct]


def topKCountries(airports, k):
    return airports['Country'].value_counts()[:k]

def boundedReachability(paths):
    reach = []
    for path in paths:
            if path not in reach:
                reach.append(path)
    reach.sort()
    return reach

def airlineAggregation():
    a = airlines.loc[airlines['Country'] == 'United States']
    return a.loc[a['Active']=='Y']

def runTests(graph, af, country, src, dst, hops, reach):
    # Country Information
    print("Showcase Countries Airports")
    start_time = time.time()
    countryAirports = getAllAirportsInCountry(af, country)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(countryAirports['Name'])
    print("--- %s seconds ---" % (time.time() - start_time))
    # Frequency
    print("Showcase Countries with most Airports (Frequency)")
    start_time = time.time()
    print("Top Country: ", topKCountries(af, 1))
    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()
    topKCountries(af, 5)
    #print("Top K=5 Countries: ", topKCountries(af, 5))
    print("--- %s seconds ---" % (time.time() - start_time))
    
    #Route information
    print("Route A to B", (src, dst))
    print("Shortest Path")
    start_time = time.time()
    nx.shortest_path(graph, source = src, target = dst)
    #print(nx.shortest_path(graph, source = src, target = dst))
    print("--- %s seconds ---" % (time.time() - start_time))

    print("Route A to B with max K hops")
    start_time = time.time()
    path = nx.all_simple_paths(graph, source = src, target = dst, cutoff=hops)
    #for pat in path:
    #   print(pat)
    print("--- %s seconds ---" % (time.time() - start_time))

    #Reachability
    print("Reachability")
    start_time = time.time()
    path = nx.single_source_shortest_path(graph, source = src, cutoff = reach+1)
    boundedReachability(list(path.keys()))
    print("--- %s seconds ---" % (time.time() - start_time))

#Airport Graph
start_time = time.time()
g = nx.DiGraph()
# Generate graph of nodes using networkX
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    if row[2] != row[4]:
        g.add_edge(row[2], row[4])
print("--- %s seconds ---" % (time.time() - start_time))
print(g.number_of_nodes())
print(g.number_of_edges())

# Create 1 Mil Graph
start_time = time.time()
test1MilRoutes = pd.read_csv('.\\PythonData\\testEdges1Mil.csv')
g1Mil = nx.DiGraph()
# Generate graph of nodes using networkX
for index, row in tqdm(test1MilRoutes.iterrows(), total=test1MilRoutes.shape[0]):
    if row[0] != row[1]:
        g1Mil.add_edge(row[0], row[1])
print("--- %s seconds ---" % (time.time() - start_time))
print(g1Mil.number_of_nodes())
print(g1Mil.number_of_edges())

# Create 5 Mil Graph
start_time = time.time()
test5MilRoutes = pd.read_csv('.\\PythonData\\testEdges5Mil.csv')
g5Mil = nx.DiGraph()
# Generate graph of nodes using networkX
for index, row in tqdm(test5MilRoutes.iterrows(), total=test5MilRoutes.shape[0]):
    if row[0] != row[1]:
        g5Mil.add_edge(row[0], row[1])
print("--- %s seconds ---" % (time.time() - start_time))
print(g5Mil.number_of_nodes())
print(g5Mil.number_of_edges())

# Create 10 Mil Graph
# start_time = time.time()
# test10MilRoutes = pd.read_csv('.\\PythonData\\testEdges10Mil.csv')
# g10Mil = nx.DiGraph()
# # Generate graph of nodes using networkX
# for index, row in tqdm(test10MilRoutes.iterrows(), total=test5MilRoutes.shape[0]):
#     if row[0] != row[1]:
#         g10Mil.add_edge(row[0], row[1])
# print("--- %s seconds ---" % (time.time() - start_time))
# print(g10Mil.number_of_nodes())
# print(g10Mil.number_of_edges())
# #graph = nx.from_pandas_edgelist(df, source='Source_Airport', target='Dest_Airport')

print("Airports")
runTests(g, af, "United States", "SEA", "DEN", 5, 2)
print("\nMid")
runTests(g1Mil, testNodesT, 1, 1, 2, 5, 2)
print("\nLarge")
runTests(g5Mil, testNodesM, 1, 1, 2, 5, 2)
#runTests(g10Mil, testNodesL, 1, 1, 2, 5, 2)

# # Running
# while(True):
#     test = input('What would you like to find out:\n1)Destination Information \n2)Trip Routes\n')
#     # Destination Information Choices
#     if test == '1':
#         print('Destination Information')
#         infochoice = input('What would you like to do:\n1)Country Airport Information \n2)Country with the most Airports\n3)Top K Countries with Airports\n4)Active Airlines in the US\n')
#         # Country airport info
#         if infochoice == '1':
#             country = input('Country: ')
#             countryAirports = getAllAirportsInCountry(af, country)
#             with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#                 print(countryAirports['Name'])
#         # Top Country
#         if infochoice == '2':
#             print('Country with the most airports:\n', topKCountries(af, 1))
#         # Top K Countries
#         if infochoice == '3':
#             k = input('How many countries would you like to see: ')
#             print('Top countries:\n', topKCountries(af, int(k)))
#         # Airline Aggregation
#         if infochoice == '4':
#             activeAirlines = airlineAggregation()
#             with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#                 print(activeAirlines['Name'])
    
#     #Route Information
#     if test == '2':
#         travelchoice = input("What would you like to do:\n1)Route from Point A to B\n2)Reachability of Airport\n")
#         # Reachability
#         if travelchoice == '1':
#             src = input('Source Aiport: ')
#             if src == 'close':
#                 break
#             dest = input('Destination Aiport: ')
#             if dest == 'close':
#                 break
#             lay = input('Max number of layovers: ')
#             if lay == '':
#                 print(nx.shortest_path(g, source = src.upper(), target = dest.upper()))
#             # Constrained Reachability
#             else:
#                 l = int(lay)+1
#                 paths = nx.all_simple_paths(g, source = src.upper(), target = dest.upper(), cutoff=int(l))
#                 for path in paths:
#                     print(path)
#         # Single Source Bounded Reachability 
#         if travelchoice == '2':
#             src = input('Source Aiport: ')
#             cut = input('Max number of hops: ')
#             cut = int(cut)+1
            # paths = nx.single_source_shortest_path(g, source = source, cutoff = x)
            # print(boundedReachability(list(paths.keys())))
#     print('\n')
