$(function() {

    $("#product_title").autocomplete({
        source: "search_bq.php",
        minLength: 3,
        select: function(event, ui) {
            var url = ui.item.id;

            //if(url != '#') {
            //    location.href = '/blog/' + url;
            //}
        },

        html: true, // optional (jquery.ui.autocomplete.html.js required)

      // optional (if other layers overlap autocomplete list)
        open: function(event, ui) {
            $(".ui-autocomplete").css("z-index", 1000);
        }
    });

});
