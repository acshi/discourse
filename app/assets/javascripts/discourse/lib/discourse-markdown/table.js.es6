import { whiteListTag } from 'discourse/lib/sanitizer';

function tableFlattenBlocks(blocks) {
  let result = "";

  blocks.forEach(b => {
    result += b;
    if (b.trailing) { result += b.trailing; }
  });

  // bypass newline insertion
  return result.replace(/[\n\r]/g, " ");
};

export function setup(helper) {
  if (Discourse.SiteSettings.allow_html_tables) {
    whiteListTag("table");
    whiteListTag("table", "class", "md-table");
    whiteListTag("tbody");
    whiteListTag("thead");
    whiteListTag("tr");
    whiteListTag("th");
    whiteListTag("td");

    helper.replaceBlock({
      start: /(<table[^>]*>)([\S\s]*)/igm,
      stop: /<\/table>/igm,
      rawContents: true,
      priority: 1,

      emitter(contents) {
        // TODO event should be fired when sanitizer loads
        if (window.html4 && window.html4.ELEMENTS.td !== 1) {
          window.html4.ELEMENTS.table = 0;
          window.html4.ELEMENTS.tbody = 1;
          window.html4.ELEMENTS.td = 1;
          window.html4.ELEMENTS.thead = 1;
          window.html4.ELEMENTS.th = 1;
          window.html4.ELEMENTS.tr = 1;
        }
        return ['table', {"class": "md-table"}, tableFlattenBlocks.apply(this, [contents])];
      }
    });
  }
}
