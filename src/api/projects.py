from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, TypeAdapter
from .common import get_bus
from src.infrastructure.message_bus import MessageBus
from src.domain import commands


router = APIRouter(prefix="/projects")


class Project(BaseModel):
    name: str
    id: str | None = None


class BasePagination[T](BaseModel):
    items: list[T]
    total: int


@router.get("/")
async def get_projects(bus: MessageBus = Depends(get_bus)):
    projects = bus.uow.projects.get_all()
    type_adapter = TypeAdapter(list[Project])

    projects = [Project(id=str(p.id), name=p.name) for p in projects]

    return BasePagination[Project](
        items=type_adapter.validate_python(projects),
        total=len(projects)
    )


@router.get("/{project_id}")
async def get_project(bus: Annotated[MessageBus, Depends(get_bus)], project_id: str):
    #project = bus.uow.projects.get(project_id)
    with bus.uow as uow:
        project = uow.projects.get(project_id)
        if not project:
            raise HTTPException(status_code=404)
        return Project.model_validate(Project(id=str(project.id), name=project.name))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_project(fa_project: Project, bus: MessageBus = Depends(get_bus)):
    project = bus.handle(commands.CreateProject(fa_project.name))
    return Project.model_validate(Project(id=str(project.id), name=project.name))


@router.patch("/{project_id}")
async def update_project(bus: Annotated[MessageBus, Depends(get_bus)], fa_project: Project, project_id: str):
    with bus.uow as uow:
        project = uow.projects.get(project_id)
        if not project:
            raise HTTPException(status_code=404)
        project.name = fa_project.name
        updated = uow.projects.update(project)
        uow.commit()
        return Project.model_validate(Project(id=str(updated.id), name=updated.name))