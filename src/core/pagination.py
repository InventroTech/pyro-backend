from django.http import QueryDict
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

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
