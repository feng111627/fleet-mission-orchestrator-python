from fleet.models import TaskSpec, TaskState


def test_models_importable():
    spec = TaskSpec(task_id="t1", capability="carry")
    assert spec.task_id == "t1"
    assert TaskState.PENDING.value == "PENDING"
