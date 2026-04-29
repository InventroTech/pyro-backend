from django.http import QueryDict
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.exceptions import NotFound

class MetaPageNumberPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 200

    def get_paginated_response(self, data):
        request = self.request
        page = self.page
        total = page.paginator.count
        page_size = self.get_page_size(request) or self.page_size
        number_of_pages = page.paginator.num_pages
        current_page = page.number

        def build_link(page_number):
            if page_number is None:
                return None
            q: QueryDict = request.query_params.copy()
            q.setlist("page", [str(page_number)])
            q.setlist("page_size", [str(page_size)])
            base = request.build_absolute_uri(request.path)
            return f"{base}?{q.urlencode()}"

        return Response({
            "data": data,
            "page_meta": {
                "total_count": total,
                "number_of_pages": number_of_pages,
                "current_page": current_page,
                "page_size": page_size,
                "next_page_link": build_link(current_page + 1 if page.has_next() else None),
                "previous_page_link": build_link(current_page - 1 if page.has_previous() else None),
            }
        })


class HasNextPageNumberPagination(PageNumberPagination):
    """
    Lightweight page-number pagination that avoids COUNT(*).
    Uses LIMIT page_size + 1 to compute has_next.
    """

    page_size = 10
    page_size_query_param = "page_size"
    max_page_size = 200

    def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        page_size = self.get_page_size(request) or self.page_size

        page_raw = request.query_params.get("page", "1")
        try:
            current_page = int(page_raw)
            if current_page < 1:
                raise ValueError
        except (TypeError, ValueError):
            raise NotFound("Invalid page.")

        offset = (current_page - 1) * page_size
        window = list(queryset[offset: offset + page_size + 1])
        self.has_next = len(window) > page_size
        self.has_previous = current_page > 1
        self.current_page = current_page
        self.page_size_value = page_size
        self.results = window[:page_size]
        return self.results

    def get_paginated_response(self, data):
        request = self.request
        current_page = self.current_page
        page_size = self.page_size_value

        def build_link(page_number):
            if page_number is None:
                return None
            q: QueryDict = request.query_params.copy()
            q.setlist("page", [str(page_number)])
            q.setlist("page_size", [str(page_size)])
            base = request.build_absolute_uri(request.path)
            return f"{base}?{q.urlencode()}"

        return Response({
            "data": data,
            "page_meta": {
                "current_page": current_page,
                "page_size": page_size,
                "has_next": self.has_next,
                "has_previous": self.has_previous,
                "next_page_link": build_link(current_page + 1 if self.has_next else None),
                "previous_page_link": build_link(current_page - 1 if self.has_previous else None),
            }
        })
