async function bootDocsSearch() {
  const input = document.getElementById("docs-search");
  const results = document.getElementById("docs-search-results");
  const indexUrl = document.body?.dataset?.searchIndex;
  if (!input || !results || !indexUrl) {
    return;
  }

  let index = [];
  try {
    const response = await fetch(indexUrl);
    index = await response.json();
  } catch (_error) {
    return;
  }

  const render = (items) => {
    results.innerHTML = "";
    if (!items.length) {
      return;
    }

    items.slice(0, 8).forEach((item) => {
      const hit = document.createElement("a");
      hit.className = "search-hit";
      hit.href = item.url;
      hit.innerHTML = `<strong>${item.title}</strong><span>${item.section}</span>`;
      results.appendChild(hit);
    });
  };

  input.addEventListener("input", () => {
    const query = input.value.trim().toLowerCase();
    if (!query) {
      results.innerHTML = "";
      return;
    }

    const matches = index.filter((item) => {
      const haystack = `${item.title} ${item.section} ${item.text}`.toLowerCase();
      return query
        .split(/\s+/)
        .every((term) => haystack.includes(term));
    });
    render(matches);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  void bootDocsSearch();
});
