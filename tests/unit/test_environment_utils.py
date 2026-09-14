from corvus_python.environment import EnvironmentUtilities


def _capture_spark_utils(monkeypatch):
    captured = []

    def fake_get_spark_utils(variable_library_name=None):
        captured.append(variable_library_name)
        return object()

    monkeypatch.setattr("corvus_python.environment.environment_utils.get_spark_utils", fake_get_spark_utils)
    return captured


class TestVariableLibraryName:
    def test_defaults_to_none_so_corvus_never_assumes_a_library(self, monkeypatch):
        captured = _capture_spark_utils(monkeypatch)

        EnvironmentUtilities()._get_spark_utils()

        assert captured == [None]

    def test_subclass_default_is_passed_through(self, monkeypatch):
        captured = _capture_spark_utils(monkeypatch)

        class ProjectEnvironmentUtilities(EnvironmentUtilities):
            variable_library_name = "project-vl"

        ProjectEnvironmentUtilities()._get_spark_utils()

        assert captured == ["project-vl"]

    def test_class_level_override_reaches_instances_created_before_it(self, monkeypatch):
        """Consuming libraries build their own instances internally, so overriding one is not enough.

        Setting the name on the class works because it is read at first use, not at construction.
        """
        captured = _capture_spark_utils(monkeypatch)

        class ProjectEnvironmentUtilities(EnvironmentUtilities):
            variable_library_name = "project-vl"

        created_earlier = ProjectEnvironmentUtilities()
        monkeypatch.setattr(ProjectEnvironmentUtilities, "variable_library_name", "notebook-vl")
        created_earlier._get_spark_utils()

        assert captured == ["notebook-vl"]
