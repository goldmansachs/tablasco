function toggleVisibility(id) {
  var summary = document.getElementById(id);
  if (summary.style.display === 'none') {
    summary.style.display = 'table-row';
  } else {
    summary.style.display = 'none';
  }
}