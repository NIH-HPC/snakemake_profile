import unittest
from bw_submit import assign_partition, make_sbatch_cmd

class TestAssignPartition(unittest.TestCase):
    def test_gpu1(self):
        "If it needs a gpu it has to go to gpu partition"
        self.assertEqual(assign_partition(2, 1024, 240, ["gpu:1"], None, None), "gpu")
    def test_gpu2(self):
        "If it needs a gpu it has to go to gpu partition"
        self.assertEqual(assign_partition(2, 1024, 240, ["gpu:v100x:1"], None, None), "gpu")
    def test_gpu3(self):
        "If it needs a gpu it has to go to gpu partition"
        self.assertEqual(assign_partition(2, 1024, 120, ["gpu:v100x:1"], None, None), "gpu")
    def test_gpu4(self):
        "If it needs a gpu it has to go to gpu partition"
        self.assertEqual(assign_partition(2, 1024, 120, ["gpu:v100x:1", "lscratch:50"], 24, 2), "gpu")
    def test_multinode(self):
        "if it has more than 16 tasks and no gpu it needs to go to multinode"
        self.assertEqual(assign_partition(2, 1024, 120, ["lscratch:50"], 24, 2), "multinode")
    def test_multinode2(self):
        "if it has more than 1 node and no gpu it needs to go to multinode"
        self.assertEqual(assign_partition(2, 1024, 120, ["lscratch:50"], None, 2), "multinode")
    def test_quick(self):
        "short jobs go to quick"
        self.assertEqual(assign_partition(2, 370 * 1024, 120, ["lscratch:100"], None, None), "quick")
    def test_mem(self):
        "short jobs go to quick if they need less than 370 gb, to norm if they need less than 499gb"
        self.assertEqual(assign_partition(2, 499 * 1024, 120, ["lscratch:100"], None, None), "norm")
    def test_mem2(self):
        "short jobs go to quick if they need less than 370 gb, to norm if they need less than 499gb"
        self.assertEqual(assign_partition(2, 500 * 1024, 120, ["lscratch:100"], None, None), "largemem")

class TestMakeSbatchCmd(unittest.TestCase):
    def test_cmd1(self):
        p = {"rule": "testrule", "params": {}, "threads": 10, 
             "resources": {"mem_mb": 1024, "disk_mb": 1000, "runtime": 600}}
        cmd, rule = make_sbatch_cmd(p)
        self.assertEqual(rule, "testrule")
        self.assertIn("--mem=1024", cmd)
        self.assertIn("--cpus-per-task=10", cmd)
        self.assertIn("--gres=lscratch:1", cmd)
    def test_cmd2(self):
        p = {"rule": "testrule2", "params": {}, "threads": 2,
                "resources": {"mem_mb": 4096, "gpu": 1, "runtime": 600}}
        cmd, rule = make_sbatch_cmd(p)
        self.assertEqual(rule, "testrule2")
        self.assertIn("--mem=4096", cmd)
        self.assertIn("--cpus-per-task=2", cmd)
        self.assertIn("--gres=gpu:1", cmd)
    def test_cmd3(self):
        p = {"rule": "testrule3", "params": {}, "threads": 2,
                "resources": {"mem_mb": 4096, "gpu": 1, "disk_mb": 4096, "runtime": 600}}
        cmd, rule = make_sbatch_cmd(p)
        self.assertEqual(rule, "testrule3")
        self.assertIn("--mem=4096", cmd)
        self.assertIn("--cpus-per-task=2", cmd)
        self.assertIn("--time=600", cmd)
        self.assertIn("--partition=gpu", cmd)
        self.assertIn("--gres=lscratch:4,gpu:1", cmd)
    def test_cmd4(self):
        "a gpu job on a a100"
        p = {"rule": "testrule3", "params": {}, "threads": 2,
                "resources": {"mem_mb": 4096, "gpu": 1, "gpu_model": "a100", "disk_mb": 4096, "runtime": 600}}
        cmd, rule = make_sbatch_cmd(p)
        self.assertEqual(rule, "testrule3")
        self.assertIn("--mem=4096", cmd)
        self.assertIn("--cpus-per-task=2", cmd)
        self.assertIn("--time=600", cmd)
        self.assertIn("--partition=gpu", cmd)
        self.assertIn("--gres=lscratch:4,gpu:a100:1", cmd)
    def test_cmd4(self):
        "test gpu constraints instead of single model"
        p = {"rule": "testrule3", "params": {}, "threads": 2,
                "resources": {"mem_mb": 4096, "gpu": 2, "gpu_model": "[gpua100|gpuv100x]", "disk_mb": 4096, "runtime": 600}}
        cmd, rule = make_sbatch_cmd(p)
        self.assertEqual(rule, "testrule3")
        self.assertIn("--mem=4096", cmd)
        self.assertIn("--cpus-per-task=2", cmd)
        self.assertIn("--time=600", cmd)
        self.assertIn("--partition=gpu", cmd)
        self.assertIn("--gres=lscratch:4,gpu:2", cmd)
        self.assertIn("--constraint='[gpua100|gpuv100x]'", cmd)

if __name__ == '__main__':
    unittest.main()
