# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining config classes for PipelineTask.
"""
__all__ = ["ResourceConfig", "PipelineTaskConfig"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig
from .connections import PipelineTaskConnections

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class PipelineTaskConfigMeta(pexConfig.ConfigMeta):
    """Metaclass used in the creation of PipelineTaskConfig classes

    This metaclass ensures a `PipelineTaskConnections` class is specified in
    the class construction parameters with a parameter name of
    pipelineConnections. Using the supplied connection class, this metaclass
    constructs a `lsst.pex.config.Config` instance which can be used to
    configure the connections class. This config is added to the config class
    under declaration with the name "connections" used as an identifier. The
    connections config also has a reference to the connections class used in
    its construction associated with an atttribute named `ConnectionsClass`.
    Finally the newly constructed config class (not an instance of it) is
    assigned to the Config class under construction with the attribute name
    `ConnectionsConfigClass`.
    """
    def __new__(cls, name, bases, dct, **kwargs):
        if name != "PipelineTaskConfig":
            # Verify that a connection class was specified and the argument is an instance of
            # PipelineTaskConfig
            if 'pipelineConnections' not in kwargs:
                for base in bases:
                    if hasattr(base, "connections"):
                        kwargs['pipelineConnections'] = base.connections.dtype.ConnectionsClass
                        break
            if 'pipelineConnections' not in kwargs:
                raise NameError("PipelineTaskConfig or a base class must be defined with connections class")
            connectionsClass = kwargs['pipelineConnections']
            if not issubclass(connectionsClass, PipelineTaskConnections):
                raise ValueError("Can only assign a PipelineTaskConnections Class to pipelineConnections")

            # Create all the fields that will be used in the newly created sub config
            # (under the attribute name "connections")
            configConnectionsNamespace = {}
            for fieldName, obj in connectionsClass.allConnections.items():
                configConnectionsNamespace[fieldName] = pexConfig.Field(dtype=str,
                                                                        doc=f"name for "
                                                                            f"connection {fieldName}",
                                                                        default=obj.name)
            # If there are default templates also add them as fields to configure the template values
            if hasattr(connectionsClass, 'defaultTemplates'):
                docString = "Template parameter used to format corresponding field template parameter"
                for templateName, default in connectionsClass.defaultTemplates.items():
                    configConnectionsNamespace[templateName] = pexConfig.Field(dtype=str,
                                                                               doc=docString,
                                                                               default=default)
            # add a reference to the connection class used to create this sub config
            configConnectionsNamespace['ConnectionsClass'] = connectionsClass

            # Create a new config class with the fields defined above
            Connections = type("Connections", (pexConfig.Config,), configConnectionsNamespace)
            # add it to the Config class that is currently being declared
            dct['connections'] = pexConfig.ConfigField(dtype=Connections,
                                                       doc='Configurations describing the'
                                                           ' connections of the PipelineTask to datatypes')
            dct['ConnectionsConfigClass'] = Connections
            dct['ConnectionsClass'] = connectionsClass
        inst = super().__new__(cls, name, bases, dct)
        return inst

    def __init__(self, name, bases, dct, **kwargs):
        # This overrides the default init to drop the kwargs argument. Python
        # metaclasses will have this argument set if any kwargs are passes at
        # class construction time, but should be consumed before calling
        # __init__ on the type metaclass. This is in accordance with python
        # documentation on metaclasses
        super().__init__(name, bases, dct)


class PipelineTaskConfig(pexConfig.Config, metaclass=PipelineTaskConfigMeta):
    """Configuration class for `PipelineTask`s

    This Configuration class functions in largely the same manner as any other
    derived from `lsst.pex.config.Config`. The only difference is in how it is
    declared. `PipelineTaskConfig` children need to be declared with a
    pipelineConnections argument. This argument should specify a child class of
    `PipelineTaskConnections`. During the declaration of a `PipelineTaskConfig`
    a config class is created with information from the supplied connections
    class to allow configuration of the connections class. This dynamically
    created config class is then attached to the `PipelineTaskConfig` via a
    `~lsst.pex.config.ConfigField` with the attribute name `connections`.
    """
    pass


class ResourceConfig(pexConfig.Config):
    """Configuration for resource requirements.

    This configuration class will be used by some activators to estimate
    resource use by pipeline. Additionally some tasks could use it to adjust
    their resource use (e.g. reduce the number of threads).

    For some resources their limit can be estimated by corresponding task,
    in that case task could set the field value. For many fields defined in
    this class their associated resource used by a task will depend on the
    size of the data and is not known in advance. For these resources their
    value will be configured through overrides based on some external
    estimates.
    """
    minMemoryMB = pexConfig.Field(dtype=int, default=None, optional=True,
                                  doc="Minimal memory needed by task, can be None if estimate is unknown.")
    minNumCores = pexConfig.Field(dtype=int, default=1,
                                  doc="Minimal number of cores needed by task.")
