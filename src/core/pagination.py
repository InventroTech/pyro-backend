from django.http import QueryDict
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


def _parse_include_count(raw):
    """Return True/False when explicit; None when param omitted."""
    if raw is None:
        return None
    return raw.strip().lower() not in ("0", "false", "no")


class MetaPageNumberPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 200

    def _include_count(self, request, view=None):
        explicit = _parse_include_count(request.query_params.get("include_count"))
        if explicit is not None:
            return explicit
        return True

    def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        page_size = self.get_page_size(request)
        if not page_size:
            return None

        if not self._include_count(request, view):
            try:
                page_number = int(request.query_params.get(self.page_query_param, 1))
            except (TypeError, ValueError):
                page_number = 1
            if page_number < 1:
                page_number = 1

            offset = (page_number - 1) * page_size
            rows = list(queryset[offset : offset + page_size + 1])
            has_next = len(rows) > page_size
            self._no_count_page = {
                "items": rows[:page_size],
                "number": page_number,
                "has_next": has_next,
                "has_previous": page_number > 1,
                "page_size": page_size,
            }
            return self._no_count_page["items"]

        self._no_count_page = None
        return super().paginate_queryset(queryset, request, view)

    def _build_page_link(self, request, page_number, page_size):
        if page_number is None:
            return None
        q: QueryDict = request.query_params.copy()
        q.setlist("page", [str(page_number)])
        q.setlist("page_size", [str(page_size)])
        base = request.build_absolute_uri(request.path)
        return f"{base}?{q.urlencode()}"

    def get_paginated_response(self, data):
        request = self.request
        page_size = self.get_page_size(request) or self.page_size

        no_count = getattr(self, "_no_count_page", None)
        if no_count:
            current_page = no_count["number"]
            return Response(
                {
                    "data": data,
                    "page_meta": {
                        "total_count": None,
                        "number_of_pages": None,
                        "current_page": current_page,
                        "page_size": no_count["page_size"],
                        "next_page_link": self._build_page_link(
                            request,
                            current_page + 1 if no_count["has_next"] else None,
                            no_count["page_size"],
                        ),
                        "previous_page_link": self._build_page_link(
                            request,
                            current_page - 1 if no_count["has_previous"] else None,
                            no_count["page_size"],
                        ),
                    },
                }
            )

        page = self.page
        total = page.paginator.count
        number_of_pages = page.paginator.num_pages
        current_page = page.number

        return Response(
            {
                "data": data,
                "page_meta": {
                    "total_count": total,
                    "number_of_pages": number_of_pages,
                    "current_page": current_page,
                    "page_size": page_size,
                    "next_page_link": self._build_page_link(
                        request,
                        current_page + 1 if page.has_next() else None,
                        page_size,
                    ),
                    "previous_page_link": self._build_page_link(
                        request,
                        current_page - 1 if page.has_previous() else None,
                        page_size,
                    ),
                },
            }
        )


class RecordListPagination(MetaPageNumberPagination):
    """
    Lead lists skip COUNT(*) by default. Pagination uses next/previous links only.
    Pass include_count=true to run a full COUNT and return total_count / number_of_pages.
    """

    def _include_count(self, request, view=None):
        explicit = _parse_include_count(request.query_params.get("include_count"))
        if explicit is not None:
            return explicit

        entity_type = (request.query_params.get("entity_type") or "").strip().lower()
        if entity_type == "lead":
            return False
        if view is not None and getattr(view, "entity_type", None) == "lead":
            return False
        return True
