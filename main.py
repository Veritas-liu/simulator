from sim import Sim
from utils import load_topo, load_routed_flows
from model import route_flows
import argparse
if __name__ == '__main__':
    # dir = './data/1'
    parser = argparse.ArgumentParser(description='Simulate the flow completion time of a given topology and flow set.')
    parser.add_argument('--dir', type=str, default='./data/1', help='The directory of the topology and flow files.')
    args = parser.parse_args()
    topo = load_topo(args.dir + '/topo.json')
    flows = load_routed_flows(args.dir + '/flow.json')
    # topo = load_topo('./Data/example/topo.json')
    # flows = load_routed_flows('./Data/example/routed_ring_flows_0_3.json')
    sim = Sim(topo)
    for flow in flows:
        if flow.dependency_count == 0:
            flow.start_time = 0
            sim.add_flow(flow)
    # step_count = 0
    # print("flow count = ", len(flows))
    while not sim.finished():
        # print("active flows:")
        # for i in sim.flows:
        #     print(i.toStr())
        finished_flow = sim.step()
        # step_count += 1
        # if step_count % 1000 == 0:
        #     print(f"step {step_count}, time {sim.time}, active flows {len(sim.flows)}")
        # print("finished flow:")
        # print(finished_flow.toStr())
        # print(sim.time)
        for flow_id in finished_flow.next_flows:
            flow = flows[flow_id]
            flow.dependency_count -= 1
            if flow.dependency_count == 0:
                flow.start_time = sim.time
                sim.add_flow(flow)
    # print("Simulation finished, total time: ", sim.time)
    with open(args.dir + '/jct.json', 'w') as f:
        f.write(str(sim.time))
