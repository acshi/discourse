import { createWidget } from 'discourse/widgets/widget';
import { emojiUrlFor } from 'pretty-text/emoji';

export default createWidget('emoji', {
  tagName: 'img.emoji',

  buildAttributes(attrs) {
    return { src: emojiUrlFor(attrs.name) };
  },
});
